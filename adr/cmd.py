import os
import docopt
import logging
from adr import *


def adr():
    '''adr : Amazon Distributed Runner

    Usage:
        adr create [--verbose=LEVEL]
        adr process <runner> [--rundir=DIR] [--verbose=LEVEL]

    Positional arguments:
        runner             runner ID

    Options:
        -h, --help         show this help message and exit
        --rundir=DIR       directory to store and run models [default: ~]
        --verbose=LEVEL    write logging messages [default: 30]

    '''

    arguments = docopt.docopt(adr.__doc__)

    # initialize file logger
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)-15s %(name)-8s %(levelname)-8s %(message)s',
                        filename='%s.log' % arguments['<runner>'])

    # initialize console logger
    console = logging.StreamHandler()
    console.setLevel(int(arguments['--verbose']))
    console.setFormatter(logging.Formatter('%(levelname)-8s %(message)s'))
    logging.getLogger('').addHandler(console)

    if arguments['create']:
        return create_runner()
    elif arguments['process']:
        while True:
            if not run_job(arguments['<runner>'], arguments['--rundir']):
                break
