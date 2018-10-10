from datetime import datetime
from datetime import date, timedelta
import dateutil.parser
from ftplib import FTP
import os
import requests
from urllib.parse import urlparse

# Our modules
import log
warn, info, debug, fatal = log.reporters()

def get_mtime(fname):
    # Get file's mtime
    mtime = datetime.fromtimestamp(os.path.getmtime(fname))  # file's mtime
    tz = time.tzname[time.localtime().tm_isdst]              # file's timezone
    return dateutil.parser.parse("%s %s" % (str(mtime), tz) ) # add timezone to file's mtime info

class Downloader():
    """This is a class to help with periodic downloading of a resource via http or ftp.

    It doesn't do ETL.  It just grabs a file."""
    def __init__(self, datadir, conn, tag="etl-download"):
        """Download items and put them in the DATADIR directory.

        CONN is a model instance we'll use to log to the database

        TAG is a string we use when logging messages in the DB.  We
        add it so we can tag all the messages related to this
        download

        """
        self.datadir = datadir
        self.conn = conn
        self.tag = tag

    def download(self, fname_url, check_log = False):
        """Download a series of urls and save them to corresponding filenames.

        FNAME_URL is a generator function that produces dicts of {"fname":"foo", "url":"bar"}"""

        downloaded = []

        for pair in fname_url(self):
            if self.dload_if_stale(pair['fname'],
                                   pair['url'],
                                   check_log = check_log):
                self.conn.log(self.tag, "Downloaded %s from %s" % (pair['fname'], pair['url']))
                downloaded.append(pair['fname'])
            else:
                debug("Not stale: %s" % pair['fname'])

        return downloaded
    def dload_if_stale(self, fname, url, check_log = False):
        """Download the file at URL and save it to FNAME, but only if the
        on-disk version is out of date.

        FNAME is the filename to save the file as

        URL is the url of the file to download

        Returns True if we downloaded, else False
        """

        if not self.fname_is_stale(fname, url, check_log=check_log):
            return False

        debug("Downloading %s from %s" % (fname, url))

        if url.startswith("ftp://"):
            return self.dload_if_stale_ftp(fname, url)

        # We stream and write this in chunks in case it is huge. It's
        # not, now, but maybe it will grow.  Better safe than sorry.
        r=requests.get(url, stream=True)

        # Warn if we can't download properly
        if r.status_code != 200:
            warn("Fetching %s returned status code of %d. Discarding result" % (url, r.status_code))
            return False

        # Did we get forwarded to a 404 page?
        if "404" in r.url:
            warn("File not found: %s" % url)
            return False

        with open(fname, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)

        assert int(r.headers["Content-Length"]) == os.path.getsize(fname)

        return True

    def dload_if_stale_ftp(self, fname, url):
        parsed_uri = urlparse(url)
        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        remote_path = parsed_uri.path
        if remote_path.startswith('/'):
            remote_path = remote_path[1:]
        domain = parsed_uri.netloc

        ftp = FTP(domain)
        ftp.login()

        try:
            with open(fname, "wb") as FH:
                cmd = "RETR " + remote_path
                ftp.retrbinary("RETR " + remote_path, FH.write)
            self.conn.log(self.tag, "Downloaded %s" % os.path.basename(fname))
            return True
        except:
            if os.path.exists(fname):
                os.unlink(fname)
            raise

    def fname_is_stale(self, fname, url, check_log = False):
        """Tries to answer the question of whether the file named FNAME needs
        to be redownloaded.

        Return True if FNAME doesn't exist.

        Return False if CHECK_LOG = True and we have a log entry in the db
        saying we already downloaded the file.

        """

        # If there is no already-existing file, it is stale
        if not os.path.exists(fname):
            return True

        # Check that we haven't already downloaded this according to the log
        if check_log:
            last = self.conn.get_download_datetime(os.path.basename(fname))
            if last != None:
                return False

        if url.startswith("ftp://"):
            stale = self.fname_is_stale_ftp(fname, url)
        else:
            stale = self.fname_is_stale_http(fname, url)

        if not stale:
            self.conn.log(self.tag, "Downloaded %s" % os.path.basename(fname))

        return stale

    def fname_is_stale_ftp(self, fname, url):
        """Tries to answer the question of whether the file named FNAME needs
        to be redownloaded.

        Return True if URL's mtime is after FNAME's mod datetime (though
        we don't handle tz because ftp is terrible)

        Return True if filesize on disk differs from filesize on the ftp server

        Return False if we can't quite download URL.

        """

        parsed_uri = urlparse(url)
        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        remote_path = parsed_uri.path
        domain = parsed_uri.netloc

        # Login to FTP server and get size and date for the file we want
        ftp = FTP(domain)
        ftp.login()
        size = ftp.size(remote_path)
        datum = ftp.sendcmd("MDTM %s" % remote_path).split(' ')[1]
        datum = datetime(int(datum[0:4]), int(datum[4:6]), int(datum[6:8]), int(datum[8:10]), int(datum[10:12]))

        # If size indicated in ftp listing differs from size on disk, then
        # the file is stale.
        if size != os.path.getsize(fname):
            warn("Size differs from that on disk. File %s is stale." % fname)
            return True

        # If the url version is somehow newer than our file on disk, the
        # file on disk is stale
        if datum > datetime.fromtimestamp(os.path.getmtime(fname)):
            return True

        # Looks like the cached file is still good
        return False

    def fname_is_stale_http (self, fname, url):
        """Tries to answer the question of whether the file named FNAME needs
        to be redownloaded.

        Return True if URL's Last-Modified is after FNAME's mod datetime.

        Return False if FNAME's mod datetime is after URL's Last-Modified.

        Return True if filesize on disk differs from filesize in the headers

        Return False if we can't quite download URL.

        """

        # Get head of url target
        r = requests.head(url)
        if r.status_code != 200:
            warn("Can't get head information about %s" % url)
            return False

        # If size indicated in header differs from size on disk, then
        # the file is stale.
        if int(r.headers["Content-Length"]) != os.path.getsize(fname):
            warn("Size differs from that on disk. File %s is stale." % fname)
            return True

        # Get mod times of url and fname
        mtime = get_mtime(fname)
        dt = dateutil.parser.parse(r.headers['Last-Modified'])   # url's last mod time

        # If the url version is newer than our file on disk, the file on
        # disk is stale
        if dt > mtime:
            return True

        # Retroactively log the download of the cached file
        self.conn.log(self.tag, "Downloaded %s" % os.path.basename(fname), mtime)

        # Looks like the cached file is still good
        return False

