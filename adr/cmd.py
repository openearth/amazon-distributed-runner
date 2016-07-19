import os
import sys
import json
import docopt
import logging
import adr


RCFILE = '~/.adrrc'


def adr_cmd():
    '''adr : Amazon Distributed Runner

Creates a queue for handling batches, launches workers to process
batches or queues batches. Also contains the processor script that
runs on the workers.

Usage:
    adr create    Create runner
    adr launch    Launch workers
    adr destroy   Destroy runner and workers
    adr queue     Queue job to runner
    adr process   Process jobs from queue
    adr list      List available runners
    adr set       Set current runner

Options:
    -h, --help         Show this help message and exit
    --verbose=LEVEL    Write logging messages [default: 30]

    '''

    if len(sys.argv) > 1:
        if sys.argv[1] == 'create':
            return adr_create()
        elif sys.argv[1] == 'launch':
            return adr_launch()
        elif sys.argv[1] == 'destroy':
            return adr_destroy()
        elif sys.argv[1] == 'queue':
            return adr_queue()
        elif sys.argv[1] == 'process':
            return adr_process()
        elif sys.argv[1] == 'list':
            return adr_list()
        elif sys.argv[1] == 'set':
            return adr_set()
        
    docopt.docopt(adr.__doc__)


def adr_create():
    '''adr_create : Create runner

Usage:
    adr create [options]
    
Options:
    -h, --help         Show this help message and exit
    --verbose=LEVEL    Write logging messages [default: 30]

    '''
    
    argv = docopt.docopt(adr_create.__doc__)
    set_logger('adr', argv['--verbose'])
    runner_id = adr.create()
    set_runner(runner_id)
    return runner_id

    
def adr_launch():
    '''adr_launch : Launch workers

Usage:
    adr launch [<runner>] [options]
    
Positional arguments:
    runner             Runner ID

Options:
    -h, --help         Show this help message and exit
    -n N               Number of workers [default: 1]
    --user=USER        SSH username
    --password=PW      SSH password
    --key=KEY          SSH key filename
    --ami=AMI          Amazon Machine Instance [default: ami-d09b6ebf]
    --asg=SG           Comma-separated list of Amazon Security Groups [default: sg-13d17c7b]
    --akp=KEY          Amazon Key Pair [default: Amazon AeoLiS Test Key]
    --ait=TYPE         Amazon Instance Type [default: t2.micro]
    --verbose=LEVEL    Write logging messages [default: 30]

    '''
    
    argv = docopt.docopt(adr_launch.__doc__)
    runner_id = get_runner(argv['<runner>'])
    set_logger(runner_id, argv['--verbose'])
    workers = adr.launch(runner_id,
                         n=argv['-n'],
                         user=argv['--user'],
                         password=argv['--password'],
                         key_filename=argv['--key'],
                         ami=argv['--ami'],
                         asg=argv['--asg'].split(','),
                         akp=argv['--akp'],
                         ait=argv['--ait'])
    
    return workers

    
def adr_destroy():
    '''adr_destroy : '''
    argv = docopt.docopt(adr_destroy.__doc__)
    runner_id = get_runner(argv['<runner>'])
    set_logger(runner_id, argv['--verbose'])
    return adr.destroy()

    
def adr_queue():
    '''adr_queue : '''
    argv = docopt.docopt(adr_queue.__doc__)
    runner_id = get_runner(argv['<runner>'])
    set_logger(runner_id, argv['--verbose'])
    return adr.queue()

    
def adr_process():
    '''adr_process : '''
    argv = docopt.docopt(adr_process.__doc__)
    runner_id = get_runner(argv['<runner>'])
    set_logger(runner_id, argv['--verbose'])
    return adr.process(runner_id)

    
def adr_list():
    '''adr_list : List available runners and hosts

Usage:
    adr list [<runner>] [options]

Positional arguments:
    runner             Runner ID

Options:
    -h, --help         Show this help message and exit
    --verbose=LEVEL    Write logging messages [default: 30]
    
    '''
    
    argv = docopt.docopt(adr_list.__doc__)
    runner_id = get_runner(argv['<runner>'])
    set_logger(runner_id, argv['--verbose'])
    return json.dumps(adr.list_workers(), indent=4)

    
def adr_set():
    '''adr_set : Set current runner

Usage:
    adr set <runner>

Positional arguments:
    runner             Runner ID

Options:
    -h, --help         Show this help message and exit
    --verbose=LEVEL    Write logging messages [default: 30]

'''
    
    argv = docopt.docopt(adr_set.__doc__)
    runner_id = get_runner(argv['<runner>'])
    return set_runner(runner_id)


def get_runner(runner_id):
    if runner_id is None:
        rcfile = os.path.expanduser(RCFILE)
        if os.path.exists(rcfile):
            with open(rcfile, 'r') as fp:
                cfg = json.load(fp)
                if cfg.has_key('runner'):
                    runner_id = cfg['runner']

    if runner_id is None:
        raise ValueError('Please specify runner ID')
    
    return runner_id


def set_runner(runner_id):
    if runner_id is not None:
        rcfile = os.path.expanduser(RCFILE)
        if os.path.exists(rcfile):
            with open(rcfile, 'r') as fp:
                cfg = json.load(fp)
        else:
            cfg = {}
        cfg['runner'] = runner_id
        with open(rcfile, 'w') as fp:
            cfg = json.dump(cfg, fp)

    return runner_id


def set_logger(name, verbosity=30):
    
    # initialize file logger
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)-15s %(name)-8s %(levelname)-8s %(message)s',
                        filename='%s.log' % name)
    
    # initialize console logger
    console = logging.StreamHandler()
    console.setLevel(verbosity)
    console.setFormatter(logging.Formatter('%(levelname)-8s %(message)s'))
    logging.getLogger('').addHandler(console)
