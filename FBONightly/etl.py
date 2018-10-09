#!/usr/bin/env python3

"""This script loads code from LEIE data files into a database.

See README.mdwn for details and instructions.

"""

# System modules
import click
from datetime import datetime
from datetime import date, timedelta
import os
import pprint
pp = pprint.PrettyPrinter(indent=4).pprint
import re
import sqlite3
import sys
import time


# Our modules
import log
import model
from path import get_datadir, get_dbdir, get_existing_file
from download import Downloader
import util as u

warn, info, debug, fatal = log.reporters()

class ETL_Helper():
    """ETL helper class for handling an incoming data file."""

    def __init__(self, db):
        """DB is a database model."""
        self.db = db


class Nightlies(ETL_Helper):
    """ETL helper class for handling nightlies."""

    def add_query(self, queries, query, record):
        if not query in queries:
            queries[query] = []
        queries[query].append(record)

    def get_nightly_records(self, fname):
        """Load the FNAME datafile, and split the nightly data into individual
        records. Return a list of those records."""
        ## Make a list of the records in the nightly data file.  We
        ## used to do this with a regex, but knowing the starting tag
        ## lets us find the ending tag with more certainty.  These
        ## files are so wonky, that relying on a regex that depends on
        ## a minimum amount blank line separation between records
        ## fails once in a while.
        records = []
        record = []
        curr_record_type = None
        nightly = u.slurp(fname, "latin-1")
        for line in nightly.split("\n"):
            if not curr_record_type:
                if not line.strip():
                    continue
                curr_record_type, undef = model.FBOTableEntry().parse_line(line)
                record.append(line)
                continue
            record.append(line)
            if line == "</%s>" % curr_record_type:
                records.append(record)
                record = []
                curr_record_type = None

        return records
    
    def etl_from_filename(self, fname):
        """Extract, translate, load data from the file named FNAME."""

        info("Parsing and loading %s" % fname)
        
        records = self.get_nightly_records(fname)
        
        unhandled = []  # Save a list of unhandled tags
        queries = {}    # Store queries so we can executemany them
        
        for record in records:

            ## Get the record type from the first tag for this record
            try:
                tag, undef = model.FBOTableEntry().parse_line(record[0])
            except model.ParseError:
                sys.stderr.write("Choked on this line: %s" % record[0])
                sys.exit(-1)

            ## Complain if we lack a class to handle this record type
            if not tag.lower() in model.get_FBOTableEntry_classes_as_dict():
                if not tag in unhandled:
                    unhandled.append(tag)
                    warn("Unhandled record type: %s" % tag)
                continue

            ## Parse the nightly-formatted text record lines into a
            ## dict.  It looks funky because we have to call the
            ## correct parser model for each type of record.  We do
            ## some module inspection to accomplish that without a
            ## giant if-then-else block.
            record = model.get_FBOTableEntry_classes_as_dict()[tag.lower()](nightly=record)

            ## Add the query to our dict of queries.  We'll do them as
            ## multi-insert transactions later because if we do the
            ## transactions one-by-one, it takes forever.
            self.add_query(queries, *self.db.make_query_dict(tag.lower(), record))

        ## Insert all the records into the database.  When doing
        ## inserts using a dict and executemany, you end up passing a
        ## query string with the insert command that specifies the
        ## columns as well as passing a list of dicts to iterate over.
        ## The insert is run once for each dict.  Here we group our
        ## inserts by common insert query.  That lets us pile up the
        ## dicts and push them through in one transaction, which is
        ## much faster than doing them as individual transactions.
        for query, records in queries.items():
            try:
                self.db.write_dict_many_query(query, records)
            except sqlite3.OperationalError:
                print(query)
                raise
            
    def etl_from_dir(self, data_dir="data", reparse=False):
        """Extract, translate, load exclusions (and not reinstatements) from
        the DATA_DIR directory."""

        # Get the data from each file in the dir
        for fname in sorted(os.listdir(data_dir)):
            if not (fname.startswith("FBO") and not fname.endswith(".sql")):
                continue
            if reparse == False and self.db.get_parsed_datetime(fname):
                continue
            self.etl_from_filename(os.path.join(data_dir,fname))
            self.db.log("nightly", "Parsed %s" % fname)
            
def date2fname(datadir, datum):
    """Take a datetime object DATUM and a string containing a path to the
    data directory and return a string with the file name of the
    nightly data file for that date.
    """
    return os.path.join(datadir, "FBOFeed%d%02d%02d" % (datum.year, datum.month, datum.day))
    
def date2url(datum):
    """Take a datetime object DATUM and return an FBO nightly ftp URL so
    we can download the file corresponding to that date."""
    return "ftp://ftp.fbo.gov/FBOFeed%d%02d%02d" % (datum.year, datum.month, datum.day)

def fname_urls(self):
    """Return a series of dicts specifying fnames and urls

    Notice the SELF parameter and that this function is not actually
    in a class.  The function gets passed to a class as a parameter,
    which then calls the function and sets self in the process.

    """

    # Start with yesterday's nightly
    x = 1

    # New files drop at about 5pm.  If we're not later than 5pm, don't
    # try to download them.  Instead, skip that date and start two
    # days ago.
    if datetime.now().hour < 17:
        x += 1

    # Each time we run this, we add download extra files to get at the backload
    maximum = len(os.listdir(self.datadir)) + 2
    
    while x < maximum:
        today = date.today() - timedelta(x)
        fname = date2fname(self.datadir, today)

        ## If we have already processed this file and created sql, the
        ## original downloaded data must be complete.  In this
        ## dataset, the data files don't change, so we're good to just
        ## skip even thinking about downloading this data file again.
        if os.path.exists(fname+".sql"):
            x += 1
            continue
        
        yield {"fname":fname, "url":date2url(today)}
        x += 1
        
@click.command()
@click.option('--reparse/--noreparse', default=False, help='Reparse old data files.')
def main(reparse):
    os.chdir(os.path.dirname(__file__))
    logger = log.logger()
    info('Starting ETL of FBO Nightly data.')

    ## Figure out where we put data
    datadir = get_datadir()
    dbdir = get_dbdir()
    if not os.path.exists(os.path.join(dbdir, "sqlite3")):
        os.makedirs(os.path.join(dbdir, "sqlite3"))
        
    ## Get a database connection, create db if needed
    db = model.FBO("development", db_conf_file=os.path.join(dbdir, "dbconf.yml"))

    ## Make sure the db schema is up to date, create tables, etc.
    db.migrate()

    assert os.path.exists(datadir)

    ## Download raw data files
    dloader = Downloader(datadir, db, 'nightly')
    dloader.download(fname_urls, True)
        
    ## Do our ETL
    nights = Nightlies(db)
    nights.etl_from_dir(reparse=reparse)
    
    ## Close the db connection
    db.close()

    info('Finished ETL of LEIE data.')

if __name__ == '__main__':
    main()
