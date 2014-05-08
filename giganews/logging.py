from __future__ import absolute_import
import logging


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
    fh.setLevel(log_level)
    formatter = logging.Formatter(FmtString)
    fh.setFormatter(formatter)
    log.addHandler(fh)
    log.addHandler(logging.StreamHandler())
    return log
