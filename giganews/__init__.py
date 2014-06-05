# -*- coding: utf-8 -*-
"""
Giganews Wrapper
~~~~~~~~~~~~~~~~~~~~~~~

usage:

>>> from giganews import NewsGroup
>>> group = NewsGroup('comp.sci.electronics')
>>> group.count
'331'

:copyright: (c) 2014 Internet Archive
:license: AGPL 3, see LICENSE for more details.

"""

__title__ = 'giganews'
__version__ = '0.0.2'
__author__ = 'Jake Johnson'
__license__ = 'AGPL 3'
__copyright__ = 'Copyright 2014 Internet Archive'

from .giganews import NewsGroup, GiganewsSession
# Set default logging handler to avoid "No handler found" warnings.
import logging
try: # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass


log = logging.getLogger(__name__)
log.addHandler(NullHandler())

def set_logger(log_level, path, logger_name='giganews'):
    """Convenience function to quickly configure any level of
    logging to a file.

    :type log_level: int
    :param log_level: A log level as specified in the `logging` module

    :type path: string
    :param path: Path to the log file. The file will be created
    if it doesn't already exist.

    """
    FmtString = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    log = logging.getLogger(logger_name)
    log.setLevel(logging.DEBUG)

    fh = logging.FileHandler(path)
    ch = logging.StreamHandler()

    #fh.setLevel(log_level)
    fh.setLevel(logging.INFO)
    ch.setLevel(logging.INFO)

    formatter = logging.Formatter(FmtString)
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    log.addHandler(fh)
    log.addHandler(ch)
    return log

set_logger(logging.WARNING, 'giganews.log')
