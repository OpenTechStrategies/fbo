"""These are two quick bits of code that set up a logging environment
and then config and return a logger instance.

In your module, do:

import log
warn, info, debug, fatal = log.reporters()

And in main, do:

logger = log.logger()

There is a second log of activities related to the database.  It is
contained in the db and the code reads and writes it to record etl
transactions.  Code for that is in dblog.py and model.py.

"""

import logging
import logging.config
import inspect
import os
import sys


def loggername():
    return os.path.basename(os.path.dirname(__file__))


def reporters():
    """Return reporting functions for various log levels."""
    lgr = logging.getLogger(loggername())
    warn = lgr.warning
    info = lgr.info
    debug = lgr.debug

    def fatal(msg):
        lgr.error(msg)
        sys.exit(-1)

    return warn, info, debug, fatal


def logger():
    """Configure and return logger.  You can override this logging by
    putting logging.ini in a parent dir of caller's python file.

    """
    #pylint: disable=protected-access

    dirname = os.path.abspath(
        os.path.dirname(
            inspect.getfile(
                sys._getframe(1))))
    if os.path.exists(os.path.join(dirname, '../logging.ini')):
        logging.config.fileConfig(os.path.join(dirname, '../logging.ini'),
                                  disable_existing_loggers=False)
    else:
        logging.config.dictConfig(dict(
            version=1,
            disable_existing_loggers=False,
            formatters={
                'f': {'format':
                      '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'}
            },
            handlers={
                'h': {'class': 'logging.StreamHandler',
                      'formatter': 'f',
                      'level': logging.DEBUG}
            },
            root={
                'handlers': ['h'],
                'level': logging.DEBUG,
            },
        ))
    return logging.getLogger(loggername())
