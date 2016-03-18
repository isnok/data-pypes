#!/usr/bin/env python
""" Usage example of the pypes micro-framework. """

from pypes import PypeSegment, PypeLine, wrap_for_next_segment

class FirstDemoProcess(PypeSegment):

    def __init__(self, name):
        super(FirstDemoProcess, self).__init__()
        self.name = name

    def process(self, stuff):
        self.log.warn('I work the stuff: %s' % stuff)
        return 'Stuff was worked...'


class SecondDemoProcess(FirstDemoProcess):

    def process(self, some, more=None):
        self.log.warn(some + ' %s', more)
        return wrap_for_next_segment(stuff='Preprocessed by %s.' % self)


combined_process = PypeLine(
    [
        FirstDemoProcess('one'),
        SecondDemoProcess('two'),
        FirstDemoProcess('three')
    ],
    name='demopipe'
)


def main():
    final = combined_process.process('Initial Stuff.')
    print("Final result: %s" % final)


if __name__ == '__main__':
    main()
