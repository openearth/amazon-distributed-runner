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
    adr prepare   Prepare workers
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
        elif sys.argv[1] == 'prepare':
            return adr_prepare()
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
        
    print adr_cmd.__doc__.strip()


def adr_create():
    '''adr_create : Create runner

Usage:
    adr create [options]
    
Options:
    -h, --help         Show this help message and exit
    --region=REGION    Amazon region [default: eu-central-1]
    --verbose=LEVEL    Write logging messages [default: 30]

    '''
    
    argv = docopt.docopt(adr_create.__doc__)
    set_logger('adr', int(argv['--verbose']))
    runner_id = adr.create(region_name=argv['--region'])
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
    --region=REGION    Amazon region [default: eu-central-1]
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
    set_logger(runner_id, int(argv['--verbose']))
    workers = adr.launch(runner_id,
                         n=argv['-n'],
                         region_name=argv['--region'],
                         user=argv['--user'],
                         password=argv['--password'],
                         key_filename=argv['--key'],
                         ami=argv['--ami'],
                         asg=argv['--asg'].split(','),
                         akp=argv['--akp'],
                         ait=argv['--ait'])
    
    return workers

    
def adr_prepare():
    '''adr_prepare : Prepare workers

Usage:
    adr prepare [<runner>] [options]
    
Positional arguments:
    runner             Runner ID

Options:
    -h, --help         Show this help message and exit
    --user=USER        SSH username [default: ubuntu]
    --password=PW      SSH password
    --key=KEY          SSH key filename
    --region=REGION    Amazon region [default: eu-central-1]
    --verbose=LEVEL    Write logging messages [default: 30]

    '''
    
    argv = docopt.docopt(adr_prepare.__doc__)
    runner_id = get_runner(argv['<runner>'])
    set_logger(runner_id, int(argv['--verbose']))
    workers = adr.get_workers(region_name=argv['--region'])
    if workers.has_key(runner_id):
        adr.prepare_workers(workers[runner_id],
                            runner_id,
                            user=argv['--user'],
                            password=argv['--password'],
                            key_filename=argv['--key'],
                            warn_only=True)


def adr_destroy():
    '''adr_destroy : Destroy runner and workers

Usage:
    adr destroy [<runner>] [options]

Positional arguments:
    runner             Runner ID

Options:
    -h, --help         Show this help message and exit
    --verbose=LEVEL    Write logging messages [default: 30]

    '''
    
    argv = docopt.docopt(adr_destroy.__doc__)
    runner_id = get_runner(argv['<runner>'])
    set_logger(runner_id, int(argv['--verbose']))
    return adr.destroy()

    
def adr_queue():
    '''adr_queue : Queue batch

Usage:
    adr queue <file>... [<runner>] [options]

Positional arguments:
    file               Input files to queue
    runner             Runner ID

Options:
    -h, --help         Show this help message and exit
    --verbose=LEVEL    Write logging messages [default: 30]

    '''
    
    argv = docopt.docopt(adr_queue.__doc__)
    runner_id = get_runner(argv['<runner>'])
    set_logger(runner_id, int(argv['--verbose']))
    return adr.queue(runner_id, argv['<file>'])

    
def adr_process():
    '''adr_process : Process jobs from queue

Usage:
    adr process [<runner>] [options]

Positional arguments:
    runner             Runner ID

Options:
    -h, --help         Show this help message and exit
    --workingdir=PATH  Working directory [default: .]
    --region=REGION    Amazon region [default: eu-central-1]
    --verbose=LEVEL    Write logging messages [default: 30]

    '''
    
    argv = docopt.docopt(adr_process.__doc__)
    runner_id = get_runner(argv['<runner>'])
    set_logger(runner_id, int(argv['--verbose']))
    return adr.process(runner_id,
                       workingdir=argv['--workingdir'],
                       region_name=argv['--region'])

    
def adr_list():
    '''adr_list : List available runners and hosts

Usage:
    adr list [<runner>] [options]

Positional arguments:
    runner             Runner ID

Options:
    -h, --help         Show this help message and exit
    --region=REGION    Amazon region [default: eu-central-1]
    --verbose=LEVEL    Write logging messages [default: 30]
    
    '''
    
    argv = docopt.docopt(adr_list.__doc__)
    runner_id = get_runner(argv['<runner>'])
    set_logger(runner_id, int(argv['--verbose']))
    return json.dumps(adr.get_workers(region_name=argv['--region']), indent=4)

    
def adr_set():
    '''adr_set : Set current runner

Usage:
    adr set <runner>

Positional arguments:
    runner             Runner ID

Options:
    -h, --help         Show this help message and exit

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

