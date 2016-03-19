""" Logging for the Pypes Framework. Can also be used 'stand-alone'.

    Logging can be configured through a bunch of environment variables:

        LOGLEVEL:
            the general loglevel
        STDOUT_LOGLEVEL:
            the loglevel of the console output
        <LEVELNAME>_LOGFILE:
            if such a variable is defined,
            a filelogger for that level is created
            and added to the logging facility

        If you run your_program.py like this:

        STDOUT_LOGLEVEL=debug LOGLEVEL=info ERROR_LOGFILE=error.log your_program.py

        Then error.log will contain all log messages of level ERROR and higher.
        On stdout you will still just see all messages from INFO on, since the
        general LOGLEVEL is set to this level, and thus messages with a lower
        loglevel will just not be handled. This is also why you should avoid
        formatting log messages right away, but let the logging facility take
        care of that instead, because then you can save the performance cost of
        formatting large data structures (say on loglevel DEBUG), by setting
        the general loglevel (when not debugging) to some higher level.

        Importing this module will also add a new level name: SUCCESS [25]
        The numeric values of that level and "it's neighbours" will then be:
        ...
        10 - DEBUG
        20 - INFO
        25 - SUCCESS
        30 - WARNING
        40 - ERROR
        ...

        For that loglevel a convenience method (logger.success) will be added
        to every logger created through setup_logger.

        >>> logger = setup_logger('test')
        >>> logger.info('Result invisible to doctest...')
"""
import os
import sys
import logging
from functools import partial

SUCCESS = 25
logging.addLevelName(SUCCESS, 'SUCCESS')

PYTHON_3 = sys.version_info[0] == 3

if PYTHON_3:
    levelnames = logging._nameToLevel
else:
    levelnames = logging._levelNames


def get_logconfig():
    """ Get logging configuration from environment.

        Returns:
            level, stdout_loglevel, files
    """

    level = os.environ.get('LOGLEVEL')

    if level is None:
        level = logging.INFO
    elif isinstance(level, (int, float)):
        level = int(level)
    elif level == str(level) and level.isdigit():
        level = int(level)
    else:
        level = logging._checkLevel(level.upper())

    files = {}

    for levelname in levelnames:
        if levelname == str(levelname):
            env_varname = levelname.upper() + '_LOGFILE'
            if env_varname in os.environ:
                files[levelname] = os.environ[env_varname]

    if 'STDOUT_LOGLEVEL' in os.environ:
        stdout_loglevel = logging._checkLevel(
            os.environ['STDOUT_LOGLEVEL'].upper()
        )
    else:
        stdout_loglevel = level

    return level, stdout_loglevel, files


loggers = {}

def setup_logger(name):
    """ Set up a custom logger with handlers for stdout and possibly some files.
        The name will be included in all it's log outputs.

        To prevent duplicate adding of handlers to already existing loggers,
        the set of created loggers is cached in the global loggers dict.
    """
    if name in loggers:
        return loggers[name]

    level, stdout_level, files = get_logconfig()

    logger = logging.getLogger(name)
    logger.setLevel(level)

    stdout_handler = logging.StreamHandler()
    stdout_handler.setLevel(stdout_level)
    readable_fmt = ' - '.join([
        '%(asctime)s',
        # '%(levelname)-4.4s',
        '[%(levelno)d] %(levelname)-8.8s',
        # '%(module)s:%(lineno)d',
        name,
        '%(message)s',
    ])
    readable_formatter = logging.Formatter(readable_fmt)
    stdout_handler.setFormatter(readable_formatter)
    logger.addHandler(stdout_handler)

    if files:
        logfile_fmt = ' - '.join([
            '%(asctime)s',
            '%(processName)s',
            # '%(levelname)-8s',
            '[%(levelno)d] %(levelname)-7s',
            '%(module)s:%(lineno)d',
            name,
            '%(message)s',
        ])
        logfile_formatter = logging.Formatter(logfile_fmt)
        for levelname, filename in files.items():
            handler = logging.FileHandler(filename, encoding='utf-8')
            handler.setLevel(levelname)
            handler.setFormatter(logfile_formatter)
            logger.addHandler(handler)

    logger.success = partial(logger.log, SUCCESS)

    return loggers.setdefault(name, logger)
