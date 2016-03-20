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
"""

from .logsetup import setup_logger

from collections import namedtuple


NextInput = namedtuple('NextInput', ['args', 'kwd'])

def wrap_for_next_segment(*args, **kwd):
    return NextInput(args, kwd)


class PypeSegment(object):

    """ A (reusable) processing step.

        The initializer should be called by subclasses
        that do real processing.

        >>> p = PypeSegment('log-signature')
        >>> str(p)
        'PypeSegment.log-signature'
        >>> p.check_inputs()
        >>> p.process()
        NextInput(args=(), kwd={})
        >>> p.process('Hello', world='World!')
        NextInput(args=('Hello',), kwd={'world': 'World!'})
    """
    name = 'default'

    def __init__(self, name=None):
        """ Set up a convenient processing environment.

            Currently this includes:
                self.__str__ - human readable string formatting
                self.log - custom logger using name as log-signature
        """

        if name is not None:
            self.name = name

        self.log = setup_logger(str(self))

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

            This method is called before the processing starts,
            and it's meaning is not fully shaped out yet.
            It's return value is ignored, and if it raises an
            exception, it will prevent the processing from being
            triggered by a Pypeline.
        """

        if False:
            msg = "{} did not supply 'required_value' as input to {}"
            raise ValueError(msg.format(previous, self))

    def process(self, *args, **kwd):
        """ Process inputs and deliver an output.

            This default implementation just wraps its arguments and passes
            them on as its output.
        """
        return wrap_for_next_segment(*args, **kwd)



class PypeLine(PypeSegment):

    """ A chain of (reusable) processing steps.

    >>> lst = [PypeSegment('noop'), PypeSegment('noop')]
    >>> pype = PypeLine(lst, name='test')
    >>> str(pype)
    'PypeLine.test'
    >>> len(pype.segments)
    2
    >>> pype.process()
    [20] - PypeLine.test - starting up
    [20] - PypeLine.test - PypeSegment.noop says input is ok
    [20] - PypeLine.test - PypeSegment.noop is done
    [20] - PypeLine.test - PypeSegment.noop says input is ok
    [20] - PypeLine.test - PypeSegment.noop is done
    [25] - PypeLine.test - output was produced.
    NextInput(args=(), kwd={})
    """

    continue_on_errors = False

    def __init__(self, segments=None, name=None, continue_on_errors=None):
        """ We add the segments argument as the new first
            argument, since it is more important, but we
            perserve the name argument.
        """
        super(PypeLine, self).__init__(name)
        self.segments = [] if segments is None else segments
        if continue_on_errors is not None:
            self.continue_on_errors = continue_on_errors

    def check_inputs(self, previous=None, *args, **kwd):
        """ Pass the check_inputs call to the first element. """
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

            try:
                # let the next segment check the input (and probably crash early)
                segment.check_inputs(previous, *data.args, **data.kwd)
                self.log.info('%s says input is ok', segment)

                # do the processing
                data = segment.process(*data.args, **data.kwd)
                self.log.info('%s is done', segment)

            except Exception:
                if self.continue_on_errors:
                    self.log.warning(
                        '%s failed, but processing will continue.', segment,
                        exc_info=True,
                    )
                else:
                    self.log.error(
                        '%s could not process %r\n',
                        segment, data,
                        exc_info=True, # add traceback information to the exception
                    )
                    raise

            # goto next
            previous = segment

        self.log.success('output was produced.')
        self.log.debug('output was %r', data)

        return data


if __name__ == '__main__':
    import doctest
    doctest.testmod()
