#!/usr/bin/env python3
"""
This module models the database tables.
"""
import datetime
import inspect
import pprint
pp = pprint.PrettyPrinter(indent=4).pprint
import re
import sys

# Import project-specific stuff
from sql import DBConn as SQL
import log
warn, info, debug, fatal = log.reporters()


class ParseError(Exception):
    pass


class UnimplementedAPI(Exception):
    pass


class FBO(SQL):
    """This is a DAO class but not an ORM class.  We're modeling the
    database, not the data.  Maybe that will change, but it works for
    now.

    """

    def goose(self, migration_sources=None):
        """Returns a dict of goose migrations.  The keys are filenames and the
        values are the contents of the goose files.

        """
        if not migration_sources:
            # Make list of migration sources
            migration_sources = [self.migrations()]
            for fbo_class in get_FBOTableEntry_classes():
                migration_sources.append(fbo_class().migrations())
        return SQL.goose(self, migration_sources)

    def unused_columns(self, tables=None):
        """TABLES is a list of tables we want to check for unused columns.
        Defaults to all the FBOTableEntry classes."""
        #pylint: disable=arguments-differ
        if not tables:
            tables = get_FBOTableEntry_classes()
        ret = {}
        for cls in tables:
            ret[cls.__name__] = SQL.unused_columns(self, cls.__name__)
        return ret


class FBOTableEntry(dict):
    """Model of a record type from the nightly fbo dump.

    This is just a dict that we're wrapping in a class so we can
    attach methods to it."""
    fields = []

    def __init__(self, nightly=None):
        dict.__init__(self)
        self.record_type = type(self).__name__.upper()
        if nightly:
            self.update(self.parse_nightly(nightly))

    def fix_date(self, date, year):
        """Combine DATE (a 4-digit integer with first two indicating the
        month and last two indicating the day) and YEAR (a two-digit
        year since 2000) into one datetime.date object

        """
        #pylint: disable=no-self-use
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
        if "link" in record:
            record.pop("link")
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
        #pylint: disable=no-self-use
        if tag.startswith('/'):
            tag = tag[1:]
        return tag in re.split(r"\s+", "a br div em h1 h2 h3 h4 h5 hr html "
                               + "label ol p span strong table tbody ul")

    def parse_line(self, line):
        """Parse out the tag from a line in a nightly data file.  Return the
        tag and the rest of the line. Does not return attributes of
        the tag.

        """
        #pylint: disable=no-self-use

        if line.startswith("<") and ">" in line:
            parts = line.split(">", 1)
            if ' ' in parts[0]:
                parts[0] = parts[0].split(' ', 1)[0]
            return parts[0][1:], parts[1]
        raise ParseError()

    def parse_nightly(self, nightly):
        """Parse a record from the nightly FBO dump, return it as a dict.

        nightly is a string (or a list of strings) containing lines of a record"""
        prev = prevprev = None
        record = {}

        # Split nightly into lines if it isn't already
        if not isinstance(nightly, list):
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

            # Skip record marking tags
            if tag in (self.record_type, "/" + self.record_type):
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
        #pylint: disable=no-self-use
        if from_name in record:
            record[to_name] = record.pop(from_name)

    def sql_table(self):
        """Return sqlite string that creates the table to hold a record for this model."""
        return ("CREATE TABLE IF NOT EXISTS %s (" %
                self.record_type +
                ", ".join(["%s %s" %
                           (x, "text" if x != "naics" else "integer") for x in self.fields]) +
                ", " +
                "sha256 text, " +
                "nightly_id text, " +
                "UNIQUE (sha256));\n")

# Below are the classes for each of the record types we encounter in
# an FBO Nightly file.  They're largely the same, but I designed the
# multi-class stuff before realizing I didn't need much custom
# parsing for any of the classes.  I'm leaving it for now because we
# might want custom parsing in the future, and the stuff in these
# classes is actually just the custom stuff anyway.


class Amdcss(FBOTableEntry):
    """Model of a Amdcss"""
    fields = re.split(
        r"\s+",
        "date solicitation_number response_date setaside " +
        "agency office location office_address zip class_code " +
        "naics subject desc url url_desc email email_desc " +
        "archive_date contact pop_address pop_zip pop_country " +
        "ntype")

    def migrations(self):
        return (("006_%s_table.sql" %
                 self.record_type, self.sql_table(), "DROP TABLE %s;" %
                 self.record_type),)


class Archive(FBOTableEntry):
    """Model of a Archive"""
    fields = re.split(
        r"\s+",
        "date solicitation_number award_number agency office location archive_date ntype")

    def migrations(self):
        return (("011_%s_table.sql" %
                 self.record_type, self.sql_table(), "DROP TABLE %s;" %
                 self.record_type),)


class Award(FBOTableEntry):
    """Model of a Award"""
    fields = re.split(
        r"\s+",
        "date solicitation_number award_number " +
        "award_amount award_date awardee line_number agency " +
        "office location office_address zip class_code naics " +
        "subject desc url url_desc email email_desc archive_date " +
        "contact ntype correction")

    def migrations(self):
        return (("008_%s_table.sql" %
                 self.record_type, self.sql_table(), "DROP TABLE %s;" %
                 self.record_type),)


class Combine(FBOTableEntry):
    """Model of a Combine"""
    fields = re.split(
        r"\s+",
        "date solicitation_number response_date setaside agency office location " +
        "office_address zip class_code naics subject desc url url_desc email " +
        "email_desc archive_date contact pop_address pop_zip pop_country")

    def migrations(self):
        return (("005_%s_table.sql" %
                 self.record_type, self.sql_table(), "DROP TABLE %s;" %
                 self.record_type),)


class Fairopp(FBOTableEntry):
    """Model of a Fairopp"""
    fields = re.split(
        r"\s+",
        "date solicitation_number award_number award_date agency office location " +
        "office_address zip class_code naics subject desc url url_desc email " +
        "email_desc archive_date contact ntype foja donbr modnbr")

    def migrations(self):
        return (("010_%s_table.sql" %
                 self.record_type, self.sql_table(), "DROP TABLE %s;" %
                 self.record_type),)


class Fstd(FBOTableEntry):
    """Model of a Fstd"""
    fields = re.split(
        r"\s+",
        "date solicitation_number response_date setaside agency office location " +
        "office_address zip class_code naics subject desc url url_desc email " +
        "email_desc contact pop_address pop_zip pop_country")

    def migrations(self):
        return (("014_%s_table.sql" %
                 self.record_type, self.sql_table(), "DROP TABLE %s;" %
                 self.record_type),)


class ITB(FBOTableEntry):
    """Model of a ITB (Invitation To Bid)"""
    fields = re.split(
        r"\s+",
        "date solicitation_number " +
        "response_date setaside agency " +
        "office location office_address zip class_code naics " +
        "subject desc url url_desc email email_desc archive_date " +
        "contact pop_address pop_zip pop_country ntype foja " +
        "donbr correction modnbr")

    def migrations(self):
        return (("015_%s_table.sql" %
                 self.record_type, self.sql_table(), "DROP TABLE %s;" %
                 self.record_type),)


class Ja(FBOTableEntry):
    """Model of a Ja"""
    fields = re.split(
        r"\s+",
        "date solicitation_number award_number award_date agency office "
        + "location office_address zip class_code naics subject desc url "
        + "url_desc email email_desc archive_date contact ntype stauth "
        + "correction modnbr")

    def migrations(self):
        return (("009_%s_table.sql" %
                 self.record_type, self.sql_table(), "DROP TABLE %s;" %
                 self.record_type),)


class Mod(FBOTableEntry):
    """Model of a Mod"""
    fields = re.split(
        r"\s+",
        "date solicitation_number response_date setaside agency office "
        + "location office_address zip class_code naics subject desc url "
        + "url_desc email email_desc archive_date contact pop_address "
        + "pop_zip pop_country ntype")

    def migrations(self):
        return (("007_%s_table.sql" %
                 self.record_type, self.sql_table(), "DROP TABLE %s;" %
                 self.record_type),)


class Presol(FBOTableEntry):
    """Model of a pre-solicitation"""
    fields = re.split(
        r"\s+",
        "date solicitation_number response_date setaside " +
        "agency office location office_address zip class_code " +
        "naics subject desc url url_desc email email_desc " +
        "archive_date contact pop_address pop_zip pop_country")

    def migrations(self):
        return (("002_%s_table.sql" %
                 self.record_type, self.sql_table(), "DROP TABLE %s;" %
                 self.record_type),)


class SNote(FBOTableEntry):
    """Model of a SNote"""
    fields = re.split(
        r"\s+",
        "date solicitation_number response_date setaside agency office " +
        "location office_address zip class_code naics subject desc url " +
        "url_desc email email_desc archive_date contact pop_address pop_zip pop_country")

    def migrations(self):
        return (("004_%s_table.sql" %
                 self.record_type, self.sql_table(), "DROP TABLE %s;" %
                 self.record_type),)


class Srcsgt(FBOTableEntry):
    """Model of a Srcsgt"""
    fields = re.split(
        r"\s+",
        "date solicitation_number response_date setaside agency " +
        "office location office_address zip class_code naics " +
        "subject desc url url_desc email email_desc archive_date " +
        "contact pop_address pop_zip pop_country")

    def migrations(self):
        return (("003_%s_table.sql" %
                 self.record_type, self.sql_table(), "DROP TABLE %s;" %
                 self.record_type),)


class SSale(FBOTableEntry):
    """Model of a SSale"""
    fields = re.split(
        r"\s+",
        "date solicitation_number response_date setaside agency " +
        "office location office_address zip class_code naics " +
        "subject desc url url_desc email email_desc archive_date " +
        "contact pop_address pop_zip pop_country")

    def migrations(self):
        return (("013_%s_table.sql" %
                 self.record_type, self.sql_table(), "DROP TABLE %s;" %
                 self.record_type),)


class Unarchive(FBOTableEntry):
    """Model of a Unarchive"""
    fields = re.split(
        r"\s+",
        "date solicitation_number award_number agency office " +
        "location archive_date ntype")

    def migrations(self):
        return (("012_%s_table.sql" %
                 self.record_type, self.sql_table(), "DROP TABLE %s;" %
                 self.record_type),)


# Here we do some inspection to extract the list of FBOTableEntry
# classes to so we can call them with a little more automated ease.
def get_FBOTableEntry_classes():
    "Inspect module and return list of FBOTableEntry classes"
    ret = []
    for _, obj in inspect.getmembers(sys.modules[__name__]):
        if (inspect.isclass(obj) and obj.__name__ != "FBOTableEntry" and
                "FBOTableEntry" in [c.__name__ for c in inspect.getmro(obj)]):
            ret.append(obj)
    return ret


def get_FBOTableEntry_classes_as_dict():
    """Inspect module and return dict of FBOTableEntry classes with class
    names keyed to the classes"""
    ret = {}
    for c in get_FBOTableEntry_classes():
        ret[c.__name__.lower()] = c
    return ret


def main(dirname=None):
    """Main entrypoint that builds and stores our models."""
    logger = log.logger()
    logger.info('Running model.py directly to produce schema/goose output.')
    conn = FBO(connect=False)
    fnames = conn.goose_write(dirname)
    logger.info(
        'Finished running model.py directly to produce schema/goose output.')
    return fnames


if __name__ == '__main__':
    main()
