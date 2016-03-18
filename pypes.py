#!/usr/bin/env python

import logging

from collections import namedtuple

NextInput = namedtuple('NextInput', ['args', 'kwd'])

def wrap_result_for_next_call(*args, **kwd):
    return NextInput(args, kwd)


class PipeSegment(object):

    """ A (reusable) processing step.

        The initializer should be called by subclasses
        that do real processing.
    """

    def __init__(self):
        """ Set up a convenient processing environment.

            Currently this includes:
                - custom logger (self.log)
        """
        logger = logging.getLogger(self.__class__.__name__)
        logger.addHandler(logging.StreamHandler())
        logger.setLevel(logging.DEBUG)
        self.logger = logger
        self.log = logger.info



    def check_inputs(self, previous=None, *args, **kwd):
        """ Called before processing, to allow early crashing.

            If self is not the first segment, the previous
            PipeSegment is given, to allow a better error
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
        return wrap_result_for_next_call()



class DataPipe(PipeSegment):

    """ A chain of (reusable) processing steps. """

    def __init__(self, segments=None):
        super(DataPipe, self).__init__()
        self.segments = [] if segments is None else segments

    def check_inputs(self, previous=None, *args, **kwd):
        if self.segments:
            return self.segments[0].check_inputs(previous, *args, **kwd)

    def process(self, *args, **kwd):
        """ Process inputs and deliver an output. """

        self.log('%s starting' % self)

        data = NextInput(args, kwd)
        previous = None

        for segment in self.segments:

            # use arbitrary return values as first argument to the process call
            if not isinstance(data, NextInput):
                data = NextInput([data], {})

            self.log('%s: input = %s' % (self, data))

            # let the next segment check the input (and probably crash early)
            segment.check_inputs(previous, *data.args, **data.kwd)

            self.log('%s: input is ok.' % segment)

            # do the processing
            data = segment.process(*data.args, **data.kwd)

            self.log('%s: result = %s' % (segment, data))

            # goto next
            previous = segment

        self.log('%s: output = %s' % (self, data))

        return data


def demo():
    """ Usage example. """

    class FirstDemoProcess(PipeSegment):

        def __init__(self, name):
            super(FirstDemoProcess, self).__init__()
            self.name = name

        def __repr__(self):
            return str(self.name)

        def process(self, stuff):
            self.log('I work the stuff: %s' % stuff)
            return 'Stuff was worked...'


    class SecondDemoProcess(FirstDemoProcess):

        def process(self, some, more=None):
            self.log(some + ' %s', more)
            return wrap_result_for_next_call(stuff='Preprocessed by %s.' % self)


    p = DataPipe([FirstDemoProcess('one'), SecondDemoProcess('two'), FirstDemoProcess('three')])

    final = p.process('Initial Stuff.')
    print("Final result: %s" % final)


if __name__ == '__main__':
    demo()
