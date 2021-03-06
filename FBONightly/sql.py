"""
This module contains routines for talking to our db, which in this case is sqlite.
"""
import datetime
import hashlib
import os
import sqlite3
import subprocess
import sys
import yaml
import dateutil.parser

# Import project-specific stuff
import log
from path import cd
import util as u

# Set up our python logging, which is separate from logging messages
# in the DB
warn, info, debug, fatal = log.reporters()


class DBNotFound(Exception):
    pass


class UnsupportedDBType(Exception):
    pass


class DBConn():
    """All the sql and goose stuff goes in this class.

    We generate the SQL here becuase in the future I think we might want some
    smart/scripted way to manage sql for different DB types.

    In addition to the parent's __init__ parameters, we need a list of
    the FBOTableEntry classes in the model.  Pass it as a kwarg.
    """

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
            with open(db_conf_file) as fh:
                self.db_conf = yaml.load(fh)[db_name]
        else:
            info("dbconf.yml not found, using default config values")
            self.db_name = "development"
            self.db_conf = yaml.load(
                "development:\n  driver: sqlite3\n  open: data.sqlite3\n")[self.db_name]

        # If we're not opening a connection, we're done
        if not connect:
            return

        # open and hang on to a db connection for later use
        if self.db_conf['driver'] == 'sqlite3':
            self.conn = sqlite3.connect(self.db_conf['open'])
        else:
            raise UnsupportedDBType(
                "We don't support databases of type %s" %
                self.db_conf['driver'])

    def close(self):
        """Commit and close the db connection"""
        self.conn.commit()
        self.conn.close()

    def table_len(self, table, columns=None):
        """Return the number of total rows in the TABLE

        If COLUMNS is specified as a list of column names, this method
        will return a second value, namely a dict of those names
        hashed to integers representing the number of rows for each
        listed name that are not empty.  If COLUMNS is specified as an
        empty list (instead of None), this method will return as the
        second value a dict with all the column names and their
        corresponding row counts.

        """
        c = self.conn.cursor()
        rows = c.execute("SELECT Count(*) FROM %s" % table).fetchone()[0]
        if columns is None:
            return rows
        ret = {}
        if columns == []:
            columns = self.get_header(table)
        for col in columns:
            ret[col] = c.execute(
                "SELECT Count(*) FROM %s WHERE %s is not null" %
                (table, col)).fetchone()[0]
        return rows, ret

    def row_to_dict(self, row, field=None, description=None):
        """
        FIELD is a list or tuple of field names

        DESCRIPTION is the results of cursor.description from sqlite

        Either FIELD or DESCRIPTION must be present, but not both.

        ROW is a tuple of values

        Returns a dict with the keys taken from FIELD and the values taken from ROW.
        """
        #pylint: disable=no-self-use
        assert field or description
        assert not (field and description)

        if description:
            field = [c[0] for c in description]

        field = ['id' if f == 'rowid' else f for f in field]
        return dict(zip(field, row))

    def make_query_dict(self, table, dic):
        """Make a query that inserts the values in the DIC into the table
        named TABLE using the keys in DIC as column names.

        """
        #pylint: disable=no-self-use

        # Add hash of all the columns so every row has a unique id
        # that survives table add/drop (but possibly not migrations)
        dic['sha256'] = hashlib.sha256(
            "|".join([str(dic) for dic in dic.values()]).encode()).hexdigest()

        # Build query
        columns = ', '.join(dic.keys())
        placeholders = ':' + ', :'.join(dic.keys())
        query = "INSERT OR IGNORE INTO %s (%s) VALUES (%s);\n" % (
            table, columns, placeholders)

        return query, dic

    def write_dict_many_query(self, query, dic):
        """Use executemany to execute a query string with each entry in dict
        DIC."""
        # Execute query
        crsr = self.conn.cursor()
        #debug("Writing %s" % ", ".join(d.keys()))
        crsr.executemany(query, dic)
        self.conn.commit()

    def write_dict(self, table, dic):
        """Write the dict D to table TABLE, where the column names correspond
        to the keys."""

        query, dic = self.make_query_dict(table, dic)

        # Execute query
        crsr = self.conn.cursor()
        #debug("Writing %s" % ", ".join(d.keys()))
        crsr.execute(query, dic)
        self.conn.commit()

    def goose(self, migration_sources=None):
        """Returns a dict of goose migrations.  The keys are filenames and the
        values are the contents of the goose files.

        MIGRATION_SOURCES is a list of tuples with three elements: the
        filename for the migrations, the SQL for upgrading, and the
        SQL for going reversing a migration.

        """
        if not migration_sources:
            migration_sources = self.migrations()
        migrations = {}
        for source in migration_sources:
            for migration in source:
                migrations[migration[0]] = "-- +goose Up\n%s\n-- +goose Down\n%s\n" % (
                    migration[1], migration[2])
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
                debug("Migration %s already exists. Overwriting.", fname)
            created.append(fname)
            info("Writing migration to %s", fname)
            with open(fname, 'w') as fh:
                fh.write(migration)
        return created

    def migrate(self):
        """Bring the db schema up to date by running any needed model
        migrations."""
        debug(self.db_conf)
        dirname = os.path.dirname(self.db_conf['open'])
        if not dirname:
            dirname = os.path.dirname(__file__)

        # Make sure our migrations are up to date
        self.goose_write()

        with cd(dirname):
            # Make sure the sqlite3 db exists before we try to migrate it
            if not os.path.exists(os.path.basename(self.db_conf['open'])):
                raise DBNotFound(
                    "DB %s doesn't exist, so we can't migrate it." %
                    self.db_conf['open'])

            # Goose apparently returns 0 even when it errors, so we
            # have to check stderr and react accordingly.
            cmd = "goose {0} {1} up".format(
                self.db_conf['driver'], os.path.basename(
                    self.db_conf['open']))
            debug("Executing `%s`", cmd)
            p = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=True)
            out, err = p.communicate()
            out = out.decode("utf-8")
            err = err.decode("utf-8")
            if p.returncode != 0:
                sys.stderr.write("%s\n%s" % (out, err))
                raise subprocess.CalledProcessError(
                    p.returncode, cmd, out + err)
            return out

    def migrations(self):
        return (("001_create_log.sql", self.sql_table(), "DROP TABLE log;"),)

    def sql_table(self):
        """Returns schema sql creating the log table.
        """

        # We only handle sqlite for now
        if self.db_conf['driver'] != "sqlite3":
            raise UnsupportedDBType(
                "We don't have migrations for %s" %
                self.db_conf['driver'])

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

        info("%s: %s", datatype, message)

        # See http://sqlite.org/datatype3.html for info on date formats in
        # sqlite3
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
        warn("Duplicate reinstatements found in %s!", table)
        info("Cleaning duplicate reinstatements from %s", table)
        c.execute(
            "delete from {0} where rowid not in (select max(rowid) from {0} group by {1})".format(
                table, ", ".join(
                    self.get_header(table))))

    def get_download_datetime(self, fname):
        """Return the logged time of the last download of the file named FNAME

        If it's not there, return None"""
        c = self.conn.cursor()
        rows = c.execute("SELECT * FROM log WHERE msg=?",
                         ["Downloaded " + fname]).fetchall()
        if not rows:
            return None
        return dateutil.parser.parse(rows[-1][0])

    def get_parsed_datetime(self, fname):
        """Return the logged time of the last parsing of the file named FNAME

        If it's not there, return None"""
        c = self.conn.cursor()
        rows = c.execute("SELECT * FROM log WHERE msg=?",
                         ["Parsed " + fname]).fetchall()
        if not rows:
            return None
        return dateutil.parser.parse(rows[-1][0])

    def get_header(self, table):
        """Returns a list of the column names in TABLE"""
        c = self.conn.cursor()
        return [
            f[1] for f in c.execute(
                "PRAGMA table_info(%s)" %
                table).fetchall()]

    def get_latest_date(self, table, field):
        """Find and return the latest month and year in the list of actions in
        TABLE by looking at dates in FIELD.  Return this value as a
        string formatted "YYYY-MM-DD".

        If there are no rows, return "".

        """

        crsr = self.conn.cursor()
        d = crsr.execute(
            "SELECT {1} FROM {0} ORDER BY date({1}) DESC Limit 1".format(
                table, field)).fetchone()
        if not d:
            return ""
        return d[0][:10]

    def get_log(self, rowid=None, limit=10, start=0, form="list"):
        """Return all the rows from the log table up to LIMIT rows

        if ROWID is set, we just return that row and LIMIT parameter
        has no effect.  If that row doesn't exist, return None.

        FORM can be 'list' or 'dict'.  If 'list', return rows as
        lists.  If dict, return rows as dicts.

        If START is specified... I dunno. not implemented yet.

        """
        assert start == 0
        assert form in ["list", "dict"]

        crsr = self.conn.cursor()

        # Return just the requested row
        if rowid:
            return crsr.execute(
                "SELECT rowid, * FROM log WHERE rowid=?",
                [rowid]).fetchone()

        # Return a range of rows
        rows = crsr.execute(
            "SELECT rowid, * FROM log ORDER BY datetime DESC LIMIT ?",
            [limit]).fetchall()
        if form == 'list':
            return rows
        return [self.row_to_dict(r, description=crsr.description)
                for r in rows]

    def unused_columns(self, table):
        """Returns a list of strings containing names of columns in TABLE that
        are empty in every row."""
        counts = self.table_len(table, [])[1]
        return [r for r in counts if counts[r] == 0]
