"""
Utility functions for FBONightly.
"""


def slurp(fname, decode="utf-8"):
    """Read file named FNAME from disk and return contents as a string.

    DECODE is the expected encoding of the file.  This func will
    decode it from bytes to a string using this setting.  It defaults
    to UTF-8.

    """
    with open(fname, 'rb') as fh:
        return fh.read().decode(decode)
