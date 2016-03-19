#!/usr/bin/env python
""" Usage example of the pypes micro-framework. """

from pypes import setup_logger
from pypes import PypeSegment
from pypes import PypeLine
from pypes import wrap_for_next_segment

main_logger = setup_logger('main')

class FirstDemoProcess(PypeSegment):

    """ Crafts a string if None given. Else it adds some processing. """

    def process(self, stuff=None):
        self.log.warn('I work the stuff: %s', stuff)
        if not stuff:
            return 'Stuff was worked...'
        else:
            return stuff + 'and processed...'

class SecondDemoProcess(PypeSegment):

    """ Adds something to the beginning of the string. """

    def process(self, some, more=None):
        self.log.warn('%s %s', some, more)
        stuff = 'Preprocessed by {} using {}: {}'.format(self, more, some)
        return wrap_for_next_segment(stuff=stuff)


combined_process = PypeLine(
    [
        FirstDemoProcess('one'),
        SecondDemoProcess('two'),
        FirstDemoProcess('three')
    ],
    name='demopipe'
)

class FailingProcess(PypeSegment):

    def check_inputs(self):
        """ Does not accept previous... """


failing_process = PypeLine(
    [
        FailingProcess(),
        FailingProcess(),
    ],
    name='demofail',
    continue_on_errors=False,
)


def main():
    print('\n==== Simple Processing ====\n')
    result = combined_process.process()
    main_logger.success("First result: %s", result)

    print('\n==== Fails expected ====\n')
    try:
        failing_process.process()
    except TypeError as ex:
        main_logger.success("Error was expected: %r" % (ex))

    print('\n==== continue_on_errors ====\n')
    failing_process.continue_on_errors = True
    failing_process.process()


if __name__ == '__main__':
    main()
