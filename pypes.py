#!/usr/bin/env python
""" The Pypes micro-framework offers you to wrap your data processing functions
    with `PypeSegment`s, that you can compose to `PypeLine`s. In return you get
    a conveniently set up logging facility, and some structure in your data flow.

    It also adds a layer of reusability to your code, but that still depends
    a great deal on how you design the inputs and outputs of your process steps.

    The framework tries to stay minimal, also in terms of what you need to know
    about it. Currently it mainly consists of two classes and one function:
        * PypeSegment:
            Inheriting from this class and defining a process method is all it
            takes. You can optionally define check_inputs, to validate your
            inputs before processing.

        * PypeLine:
            A chain of PypeSegments. It can be used as a Segment itself.

        * wrap_for_next_segment():
            Call this, how you would call the next processing function and
            return its result. It will create a wrapper object to ship the
            results/arguments through the PypeLine code.

    It can be configured through one environment variable or method:
"""

import os
import logging

def get_loglevel(level=None):

    if level is None:
        level = os.environ.get('LOGLEVEL')

    if level is None:
        return logging.INFO
    if isinstance(level, (int, float)):
        return int(level)
    elif level == str(level) and level.isdigit():
        return int(level)
    else:
        return logging._checkLevel(level.upper())

from functools import partial
from collections import namedtuple

NextInput = namedtuple('NextInput', ['args', 'kwd'])

def wrap_for_next_segment(*args, **kwd):
    return NextInput(args, kwd)


class PypeSegment(object):

    """ A (reusable) processing step.

        The initializer should be called by subclasses
        that do real processing.

        >>> p = PypeSegment('log-signature')
        >>> p.check_inputs()
        >>> p.process()
        NextInput(args=(), kwd={})

    """
    name = 'default'

    def __init__(self, name=None):
        """ Set up a convenient processing environment.

            Currently this includes:
                - custom logger (self.log)
                - readable log format, if loglevel is info, detailed else
        """

        if name is not None:
            self.name = name

        self.init_logging()

    def init_logging(self):

        logger = self.log = logging.getLogger(str(self))

        # default stream is stdout:
        handler = logging.StreamHandler()

        level = get_loglevel()
        logger.setLevel(level)

        if logging.DEBUG < level <= logging.INFO:
            fmt = ' - '.join([
                '%(asctime)s',
                # '%(levelname)-4.4s',
                # '%(module)s:%(lineno)d',
                str(self),
                '%(message)s',
            ])
        else:
            fmt = ' - '.join([
                '%(asctime)s',
                '%(processName)s',
                '%(levelname)-8s',
                '%(module)s:%(lineno)d',
                str(self),
                '%(message)s',
            ])
        formatter = logging.Formatter(fmt)

        handler.setFormatter(formatter)
        logger.addHandler(handler)
        # logger.error('loglevel: %s', level)

    def __str__(self):
        return '{}.{}'.format(
            self.__class__.__name__,
            self.name,
        )

    def check_inputs(self, previous=None, *args, **kwd):
        """ Called before processing, to allow early crashing.

            If self is not the first segment, the previous
            PypeSegment is given, to allow a better error
            message or warning.
        """

        if False:
            msg = "{} did not supply 'required_value' as input to {}"
            raise ValueError(msg.format(previous, self))

    def process(self, *args, **kwd):
        """ Process inputs and deliver an output.

            This default implementation shows how to
            return absolutely nothing, not even a None
            (which would be one argument to the next segment)
            in the DataPipe logic.
        """
        return wrap_for_next_segment()



class PypeLine(PypeSegment):

    """ A chain of (reusable) processing steps.
    """

    def __init__(self, segments=None, name=None):
        """ We add the segments argument as the new first
            argument, since it is more important, but we
            perserve the name argument.
        """
        super(PypeLine, self).__init__(name)
        self.segments = [] if segments is None else segments

    def check_inputs(self, previous=None, *args, **kwd):
        if self.segments:
            return self.segments[0].check_inputs(previous, *args, **kwd)

    def process(self, *args, **kwd):
        """ Process inputs and deliver an output. """

        self.log.info('starting up')

        data = NextInput(args, kwd)
        previous = None

        for segment in self.segments:

            # use arbitrary return values as first argument to the process call
            if not isinstance(data, NextInput):
                data = NextInput([data], {})

            self.log.debug('next input: %s', data)

            # let the next segment check the input (and probably crash early)
            segment.check_inputs(previous, *data.args, **data.kwd)

            self.log.info('%s says: input is ok.' % segment)

            # do the processing
            data = segment.process(*data.args, **data.kwd)

            self.log.info('%s: result = %s' % (segment, data))

            # goto next
            previous = segment

        self.log.info('output = %s', data)

        return data



if __name__ == '__main__':
    import doctest
    doctest.testmod()
