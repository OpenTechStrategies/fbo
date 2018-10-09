#!/usr/bin/env python3

import datetime
import dateutil.parser
import hashlib
import inspect
import os
from path import cd
import pprint
pp = pprint.PrettyPrinter(indent=4).pprint
import simplejson as json
import sqlite3
import subprocess
import sys
import yaml

import log
warn, info, debug, fatal = log.reporters()

# Import project-specific stuff
import util as u

class ParseError(Exception):
    pass
class UnimplementedAPI(Exception):
    pass
class UnsupportedDBType(Exception):
    pass
class DBNotFound(Exception):
   pass

class DBConn(object):
    def __init__(self, db_name="development", db_conf_file="", connect=True):
        """Open a database connection, creating db if needed, and generally
        get ready to store stuff.

        DB_NAME is the name of the database to target from dbconf.yml.

        If DB_CONF_FILE isn't specified, we use a stock one of defaults.

        Goose migrations used dbconf.yml files, so for convenience, we
        just read any needed data from that file.

        If CONNECT is true, we open a db connection.

        """
        self.db_name = db_name
        if os.path.exists(db_conf_file):
            # slurp dbconf.yml
            with open(db_conf_file) as INF:
                self.db_conf = yaml.load(INF)[db_name]
        else:
            info("dbconf.yml not found, using default config values")
            self.db_name = "development"
            self.db_conf = yaml.load("development:\n  driver: sqlite3\n  open: data.sqlite3\n")[self.db_name]

        # If we're not opening a connection, we're done
        if not connect:
            return

        # open and hang on to a db connection for later use
        if self.db_conf['driver'] == 'sqlite3':
            self.conn = sqlite3.connect(self.db_conf['open'])
        else:
            raise UnsupportedDBType("We don't support databases of type %s" % self.db_conf['driver'])

    def close(self):
        """Commit and close the db connection"""
        self.conn.commit()
        self.conn.close()

    def table_len(self, table):
        """Return the number of total rows in the TABLE"""
        c = self.conn.cursor()
        return (c.execute("SELECT Count(*) FROM %s" % table).fetchone()[0])

    def row_to_dict(self, row, field=None, description=None):
        """
        FIELD is a list or tuple of field names

        DESCRIPTION is the results of cursor.description from sqlite

        Either FIELD or DESCRIPTION must be present, but not both.

        ROW is a tuple of values

        Returns a dict with the keys taken from FIELD and the values taken from ROW.
        """
        assert field or description
        assert not (field and description)

        if description:
            field = [c[0] for c in description]

        field = ['id' if f == 'rowid' else f for f in field]
        return dict(zip(field, row))

    def make_query_dict(self, table, d):
        ## Add hash of all the columns so every row has a unique id
        ## that survives table add/drop (but possibly not migrations)
        d['sha256'] =  hashlib.sha256("|".join([str(d) for d in d.values()]).encode()).hexdigest()
        
        ## Build query
        columns = ', '.join(d.keys())
        placeholders = ':'+', :'.join(d.keys())
        query = "INSERT OR IGNORE INTO %s (%s) VALUES (%s);\n" % (table, columns, placeholders)

        return query, d
    
    def write_dict_many_query(self, query, d):
        ## Execute query
        crsr = self.conn.cursor()
        #debug("Writing %s" % ", ".join(d.keys()))
        crsr.executemany(query, d)
        self.conn.commit()
        
    def write_dict(self, table, d):
        """Write the dict D to table TABLE, where the column names correspond
        to the keys."""

        query, d = self.make_query_dict(table, d)

        ## Execute query
        crsr = self.conn.cursor()
        #debug("Writing %s" % ", ".join(d.keys()))
        crsr.execute(query, d)
        self.conn.commit()

class SQL(DBConn):
    """All the sql and goose stuff goes in this class.

    We generate the SQL here becuase in the future I think we might want some
    smart/scripted way to manage sql for different DB types."""

    def goose(self):
        """Returns a dict of goose migrations.  The keys are filenames and the
        values are the contents of the goose files.

        We only have one migration so far, so this is pretty easy.
        """

        # Make list of migration sources
        migration_sources = [self.migrations()]
        for fbo_class in get_FBOTableEntry_classes():
            migration_sources.append(fbo_class().migrations())
            
        migrations = {}
        for source in migration_sources:
            for migration in source:
                migrations[migration[0]] = "-- +goose Up\n%s\n-- +goose Down\n%s\n" % (migration[1], migration[2])
        return migrations
    
    def goose_write(self, dirname=None):
        """Writes any needed migration files to the migrations directory
        specified by DIRNAME.  Leave DIRNAME as None to just use
        ./db as the migrations directory.

        Returns list of paths to created files.
        """
        if not dirname:
            dirname = os.path.join(os.path.dirname(__file__), "db")
        dirname = os.path.join(dirname, self.db_conf['driver'])
        os.makedirs(dirname, exist_ok=True)
        created = []
        for fname, migration in self.goose().items():
            fname = os.path.join(dirname, fname)
            if os.path.exists(fname):
                if u.slurp(fname) == migration:
                    continue
                debug("Migration " +fname+" already exists. Overwriting.")
            created.append(fname)
            info("Writing migration to " + fname)
            with open(fname, 'w') as OUTF:
                OUTF.write(migration)
        return created

    def migrate(self):
        """Bring the db schema up to date by running any needed model
        migrations."""
        debug(self.db_conf)
        dirname = os.path.dirname(self.db_conf['open'])
        if not dirname:
            dirname = os.path.dirname(__file__)

        ## Make sure our migrations are up to date
        self.goose_write()
        
        with cd(dirname):
            # Make sure the sqlite3 db exists before we try to migrate it
            if not os.path.exists(os.path.basename(self.db_conf['open'])):
                raise DBNotFound("DB %s doesn't exist, so we can't migrate it." % self.db_conf['open'])

            # Goose apparently returns 0 even when it errors, so we
            # have to check stderr and react accordingly.
            cmd = "goose {0} {1} up".format(self.db_conf['driver'], os.path.basename(self.db_conf['open']))
            debug("Executing `%s`" % cmd)
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            out, err = p.communicate()
            out = out.decode("utf-8")
            err = err.decode("utf-8")
            if p.returncode != 0:
                sys.stderr.write("%s\n%s" % (out, err))
                raise subprocess.CalledProcessError(p.returncode, cmd, out+err)
            return out
        
    def migrations(self):
        return (("001_create_log.sql", self.sql_table(), "DROP TABLE log;"),)

    def sql_table(self):
        """Returns schema sql creating the log table.
        """

        # We only handle sqlite for now
        if self.db_conf['driver'] != "sqlite3":
            raise UnsupportedDBType("We don't have migrations for %s" % self.db_conf['driver'])

        return """CREATE TABLE IF NOT EXISTS log (
        datetime text,
        datatype text,
        msg text);
        """
        

    def log(self, datatype, message, now=""):
        """Add a MESSAGE string about a DATATYPE (either updated or
        reinstatement) to the log table in the db.

        Else, NOW = a datestring we can parse.  It can be anything
        whose str representation is a parseable datetime, including a
        datetime.

        """

        info("%s: %s" % (datatype, message))

        # See http://sqlite.org/datatype3.html for info on date formats in sqlite3
        if not now:
            now = datetime.datetime.now().isoformat()
        else:
            now = dateutil.parser.parse(str(now)).isoformat()

        crsr = self.conn.cursor()
        crsr.execute("INSERT INTO log VALUES(?,?,?)", (now, datatype, message))
        self.conn.commit()

    def dedupe(self, table):
        """
        Remove any duplicate rows from TABLE
        """

        # Look for duplicate entries
        seen = set()
        uniq = []
        dup = []
        c = self.conn.cursor()
        for x in c.execute("SELECT * FROM %s" % table).fetchall():
            if x not in seen:
                uniq.append(x)
                seen.add(x)
            else:
                dup.append(x)

        # We're done if there are no dupes
        if not dup:
            return

        # Uh-oh, better fess up and clean up
        warn("Duplicate reinstatements found in %s!" % table)
        info("Cleaning duplicate reinstatements from %s" % table)
        c.execute("delete from {0} where rowid not in (select max(rowid) from {0} group by {1})".format(
            table,
            ", ".join(self.get_header(table))
            ))

    def get_download_datetime(self, fname):
        """Return the logged time of the last download of the file named FNAME

        If it's not there, return None"""
        c = self.conn.cursor()
        all = c.execute("SELECT * FROM log WHERE msg=?", ["Downloaded " + fname]).fetchall()
        if not all:
            return None
        return dateutil.parser.parse(all[-1][0])

    def get_header(self, table):
        """Returns a list of the column names in TABLE"""
        c = self.conn.cursor()
        return [f[1] for f in c.execute("PRAGMA table_info(%s)" % table).fetchall()]

    def get_latest_date(self, table, field):
        """Find and return the latest month and year in the list of actions in
        TABLE by looking at dates in FIELD.  Return this value as a
        string formatted "YYYY-MM-DD".

        If there are no rows, return "".

        """

        crsr = self.conn.cursor()
        d = crsr.execute("SELECT {1} FROM {0} ORDER BY date({1}) DESC Limit 1".format(table, field)).fetchone()
        if not d:
            return ""
        return d[0][:10]

    def get_log(self, rowid=None, limit=10, start=0, form="list"):
        """Return all the rows from the log table up to LIMIT rows

        if ROWID is set, we just return that row and LIMIT parameter has no effect.  If that row doesn't exist, return None.

        FORM can be 'list' or 'dict'.  If 'list', return rows as lists.  If dict, return rows as dicts.

        If START is specified... I dunno. not implemented yet.
        """

        assert form in ["list", "dict"]

        crsr = self.conn.cursor()

        # Return just the requested row
        if rowid:
            return crsr.execute("SELECT rowid, * FROM log WHERE rowid=?", [rowid]).fetchone()

        # Return a range of rows
        rows = crsr.execute("SELECT rowid, * FROM log ORDER BY datetime DESC LIMIT ?", [limit]).fetchall()
        if form == 'list':
            return rows
        return [self.row_to_dict(r, description=crsr.description) for r in rows]

    def unused_columns(self, table):
        """Returns a list of strings containing names of columns in TABLE that
        are empty in every row."""
        pass
    
class FBO(SQL):
    """This is a DAO class but not an ORM class.  We're modeling the
    database, not the data.  Maybe that will change, but it works for
    now.

    """
    pass

class FBOTableEntry(dict):
    """Model of a record type from the nightly fbo dump.

    This is just a dict that we're wrapping in a class so we can
    attach methods to it."""
    def __init__(self, nightly=None):
        self.record_type = type(self).__name__.upper()
        if nightly:
            self.update( self.parse_nightly(nightly))
            
    def fix_date(self, date, year):
        """Combine DATE (a 4-digit integer with first two indicating the
        month and last two indicating the day) and YEAR (a two-digit
        year since 2000) into one datetime.date object

        """
        year = int(year)
        if year < 90:
            year += 2000
        elif year <= 99:
            year += 1900
        return datetime.date(year, int(date[0:2]), int(date[2:4]))

    def cleanup(self, record):
        """RECORD is a dict that contains data extracted from a nightly
        record.  This method cleans up that data by
        renaming/deleting/combining fields, adjusting formats, etc.

        """
        if "link" in record: record.pop("link")
        if "respdate" in record:
            date = record.pop("respdate")
            record["response_date"] = self.fix_date(date[0:4], date[4:6])
            
        self.rename_field(record, "address", "email")
        self.rename_field(record, "awdamt", "award_amount")
        self.rename_field(record, "awddate", "award_date")
        self.rename_field(record, "awdnbr", "award_number")
        self.rename_field(record, "archdate", "archive_date")
        self.rename_field(record, "classcod", "class_code")
        self.rename_field(record, "linenbr", "line_number")
        self.rename_field(record, "offadd", "office_address")
        self.rename_field(record, "popcountry", "pop_country")
        self.rename_field(record, "popzip", "pop_zip")
        self.rename_field(record, "popaddress", "pop_address")
        self.rename_field(record, "solnbr", "solicitation_number")
        record['date'] = self.fix_date(record['date'], record.pop('year'))
        return record
    
    def is_ignored_tag(self, tag):
        "These are mostly html, but not all.  Returns TRUE if the tag is on our ignore list."
        if tag.startswith('/'):
            tag = tag[1:]
        return tag in "a br div em h1 h2 h3 h4 h5 hr html label ol p span strong table tbody ul".split(' ')

    def parse_line(self, line):
        if line.startswith("<") and ">" in line:
            parts = line.split(">",1)
            if ' ' in parts[0]:
                parts[0] = parts[0].split(' ',1)[0]
            return parts[0][1:], parts[1]
        else:
            raise ParseError()

    def parse_nightly(self, nightly):
        """Parse a record from the nightly FBO dump, return it as a dict.

        nightly is a string (or a list of strings) containing lines of a record"""
        prev = prevprev = None
        record = {}

        ## Split nightly into lines if it isn't already
        if type(nightly) != list:
            nightly = nightly.strip().split("\n")

        for line in nightly:
            if line.endswith("\r"):
                record[prev.lower()] += line
                continue
            
            try:
                tag, val = self.parse_line(line)
            except ParseError:
                # No tag means we just continue the previous one
                record[prev.lower()] += line
                continue
            
            ## Skip record marking tags
            if (tag == self.record_type or tag == "/"+self.record_type):
                continue
            
            # Handle the desc fields, which repeat but have different
            # meanings based on content two lines above them.
            if tag == "DESC":
                if prevprev == "LINK":
                    record['url_desc'] = val
                    prev = 'url_desc'
                    continue
                elif prevprev == "EMAIL":
                    record['email_desc'] = val
                    prev = 'email_desc'
                    continue
                else:
                    record['desc'] = val
            elif self.is_ignored_tag(tag):
                record[prev.lower()] += "\n" + line
                continue
            else:
                record[tag.lower()] = val
            prevprev = prev
            prev = tag

        return self.cleanup(record)
    
    def rename_field(self, record, from_name, to_name):
        if from_name in record:
            record[to_name] = record.pop(from_name)

    def sql_table(self):
        """Return sqlite string that creates the table to hold a record for this model."""
        return ("CREATE TABLE IF NOT EXISTS %s (" % self.record_type
                + ", ".join(["%s %s" % (x, "text" if x != "naics" else "integer") for x in self.fields]) + ", "
                + "sha256 text, "
                + "nightly_id text, "
                + "UNIQUE (sha256));\n")
    
    
## Below are the classes for each of the record types we encounter in
## an FBO Nightly file.  They're largely the same, but I designed the
## multi-class stuff before realizing I didn't need much custom
## parsing for any of the classes.  I'm leaving it for now because we
## might want custom parsing in the future, and the stuff in these
## classes is actually just the custom stuff anyway.
class Amdcss(FBOTableEntry):
    """Model of a Amdcss"""
    fields = "date solicitation_number response_date setaside agency office location office_address zip class_code naics subject desc url url_desc email email_desc archive_date contact pop_address pop_zip pop_country ntype".split(" ")
    
    def migrations(self):
        return (("006_%s_table.sql" % self.record_type, self.sql_table(), "DROP TABLE %s;" % self.record_type),)

class Archive(FBOTableEntry):
    """Model of a Archive"""
    fields = "date solicitation_number award_number award_amount award_date response_date setaside agency office location office_address zip class_code naics subject desc url url_desc email email_desc archive_date contact pop_address pop_zip pop_country ntype foja donbr correction modnbr".split(" ")
    
    def migrations(self):
        return (("011_%s_table.sql" % self.record_type, self.sql_table(), "DROP TABLE %s;" % self.record_type),)

class Award(FBOTableEntry):
    """Model of a Award"""
    fields = "date solicitation_number award_number award_amount award_date awardee line_number response_date setaside agency office location office_address zip class_code naics subject desc url url_desc email email_desc archive_date contact pop_address pop_zip pop_country ntype correction".split(" ")
    
    def migrations(self):
        return (("008_%s_table.sql" % self.record_type, self.sql_table(), "DROP TABLE %s;" % self.record_type),)

class Combine(FBOTableEntry):
    """Model of a Combine"""
    fields = "date solicitation_number response_date setaside agency office location office_address zip class_code naics subject desc url url_desc email email_desc archive_date contact pop_address pop_zip pop_country".split(" ")
    
    def migrations(self):
        return (("005_%s_table.sql" % self.record_type, self.sql_table(), "DROP TABLE %s;" % self.record_type),)
    
        
class Fairopp(FBOTableEntry):
    """Model of a Fairopp"""
    fields = "date solicitation_number award_number award_amount award_date response_date setaside agency office location office_address zip class_code naics subject desc url url_desc email email_desc archive_date contact pop_address pop_zip pop_country ntype foja donbr correction modnbr".split(" ")
    
    def migrations(self):
        return (("010_%s_table.sql" % self.record_type, self.sql_table(), "DROP TABLE %s;" % self.record_type),)

class Fstd(FBOTableEntry):
    """Model of a Fstd"""
    fields = "date solicitation_number award_number award_amount award_date response_date setaside agency office location office_address zip class_code naics subject desc url url_desc email email_desc archive_date contact pop_address pop_zip pop_country ntype foja donbr correction modnbr".split(" ")
    
    def migrations(self):
        return (("014_%s_table.sql" % self.record_type, self.sql_table(), "DROP TABLE %s;" % self.record_type),)

class Ja(FBOTableEntry):
    """Model of a Ja"""
    fields = "date solicitation_number award_number award_amount award_date response_date setaside agency office location office_address zip class_code naics subject desc url url_desc email email_desc archive_date contact pop_address pop_zip pop_country ntype stauth correction modnbr".split(" ")
    
    def migrations(self):
        return (("009_%s_table.sql" % self.record_type, self.sql_table(), "DROP TABLE %s;" % self.record_type),)

class Mod(FBOTableEntry):
    """Model of a Mod"""
    fields = "date solicitation_number response_date setaside agency office location office_address zip class_code naics subject desc url url_desc email email_desc archive_date contact pop_address pop_zip pop_country ntype".split(" ")
    
    def migrations(self):
        return (("007_%s_table.sql" % self.record_type, self.sql_table(), "DROP TABLE %s;" % self.record_type),)

class Presol(FBOTableEntry):
    """Model of a pre-solicitation"""
    fields = "date solicitation_number response_date setaside agency office location office_address zip class_code naics subject desc url url_desc email email_desc archive_date contact pop_address pop_zip pop_country".split(" ")
    
    def migrations(self):
        return (("002_%s_table.sql" % self.record_type, self.sql_table(), "DROP TABLE %s;" % self.record_type),)
    
class SNote(FBOTableEntry):
    """Model of a SNote"""
    fields = "date solicitation_number response_date setaside agency office location office_address zip class_code naics subject desc url url_desc email email_desc archive_date contact pop_address pop_zip pop_country".split(" ")
    
    def migrations(self):
        return (("004_%s_table.sql" % self.record_type, self.sql_table(), "DROP TABLE %s;" % self.record_type),)
    
class Srcsgt(FBOTableEntry):
    """Model of a Srcsgt"""
    fields = "date solicitation_number response_date setaside agency office location office_address zip class_code naics subject desc url url_desc email email_desc archive_date contact pop_address pop_zip pop_country".split(" ")
    
    def migrations(self):
        return (("003_%s_table.sql" % self.record_type, self.sql_table(), "DROP TABLE %s;" % self.record_type),)

class SSale(FBOTableEntry):
    """Model of a SSale"""
    fields = "date solicitation_number response_date setaside agency office location office_address zip class_code naics subject desc url url_desc email email_desc archive_date contact pop_address pop_zip pop_country".split(" ")
    
    def migrations(self):
        return (("013_%s_table.sql" % self.record_type, self.sql_table(), "DROP TABLE %s;" % self.record_type),)
    
class Unarchive(FBOTableEntry):
    """Model of a Unarchive"""
    fields = "date solicitation_number award_number award_amount award_date response_date setaside agency office location office_address zip class_code naics subject desc url url_desc email email_desc archive_date contact pop_address pop_zip pop_country ntype foja donbr correction modnbr".split(" ")
    
    def migrations(self):
        return (("012_%s_table.sql" % self.record_type, self.sql_table(), "DROP TABLE %s;" % self.record_type),)


## Here we do some inspection to extract the list of FBOTableEntry
## classes to so we can call them with a little more automated ease.
def get_FBOTableEntry_classes():
    ret = []
    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if (inspect.isclass(obj)
            and obj.__name__ != "FBOTableEntry"
            and "FBOTableEntry" in [c.__name__ for c in inspect.getmro(obj)]):
            ret.append(obj)
    return ret
def get_FBOTableEntry_classes_as_dict():
    ret = {}
    for c in get_FBOTableEntry_classes():
        ret[c.__name__.lower()] = c
    return ret
FBOTableEntry_classes = get_FBOTableEntry_classes_as_dict()

def main(dirname=None):
    logger = log.logger()
    logger.info('Running model.py directly to produce schema/goose output.')
    conn = SQL(connect=False)
    fnames = conn.goose_write(dirname)
    logger.info('Finished running model.py directly to produce schema/goose output.')
    return fnames

if __name__ == '__main__':
    main()
