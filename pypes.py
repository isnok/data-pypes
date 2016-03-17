#!/usr/bin/env python

from collections import namedtuple

NextInput = namedtuple('NextInput', ['args', 'kwd'])

class PipeSegment(object):

    """ A (reusable) processing step.

        The initializer is left out here on purpose,
        so that it can be used by the subclasses that
        do real processing.
    """

    def check_inputs(self, previous=None, *args, **kwd):
        """ Called before processing, to allow early crashing.

            If existing, the previous PipeSegment is given, to allow
            for a better error message.
        """

        if False:
            msg = "{} did not supply 'required_value' as input to {}"
            raise ValueError(msg.format(previous, self))

    def process(self, *args, **kwd):
        """ Process inputs and deliver an output. """

    def log(self, *args):
        """ TODO: use real logging """
        print ' '.join([str(a) for a in args])


class DataPipe(object):

    """ A chain of (reusable) processing steps. """

    def __init__(self, segments=None):
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

        return data

    def log(self, *args):
        """ TODO: use real logging """
        print ' '.join([str(a) for a in args])


def demo():
    """ Usage example. """

    class FirstDemoProcess(PipeSegment):

        def __init__(self, name):
            self.name = name

        def __repr__(self):
            return str(self.name)

        def process(self, stuff):
            self.log(self, 'I work the stuff: %s' % stuff)
            return 'Stuff was worked...'


    class SecondDemoProcess(FirstDemoProcess):

        def process(self, some, more=None):
            self.log(self, some, more)
            return NextInput([], {'stuff': 'Preprocessed by %s.' % self})


    p = DataPipe([FirstDemoProcess('one'), SecondDemoProcess('two'), FirstDemoProcess('three')])

    final = p.process('Initial Stuff.')
    print("Final result: %s" % final)


if __name__ == '__main__':
    demo()
