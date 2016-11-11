import os
import re
import sys
import json
import docopt
import logging
import adr, config


def adr_console():
    '''adr : Amazon Distributed Runner

Creates a queue for handling batches, launches workers to process
batches or queues batches. Also contains the processor script that
runs on the workers.

Usage:
    adr create    Create runner
    adr launch    Launch workers
    adr prepare   Prepare workers
    adr start     Start workers
    adr stop      Stop workers
    adr destroy   Destroy runner and workers
    adr queue     Queue batch to runner
    adr process   Process batches from queue
    adr download  Download batch results
    adr list      List available runners
    adr set       Set current runner
    adr config    Configuration wizard

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
        elif sys.argv[1] == 'start':
            return adr_start()
        elif sys.argv[1] == 'stop':
            return adr_stop()
        elif sys.argv[1] == 'destroy':
            return adr_destroy()
        elif sys.argv[1] == 'queue':
            return adr_queue()
        elif sys.argv[1] == 'process':
            return adr_process()
        elif sys.argv[1] == 'download':
            return adr_download()
        elif sys.argv[1] == 'list':
            return adr_list()
        elif sys.argv[1] == 'set':
            return adr_set()
        elif sys.argv[1] == 'config':
            return adr_config()
        
    print adr_console.__doc__.strip()


def adr_create():
    '''adr_create : Create runner

Usage:
    adr create [options]
    
Options:
    -h, --help         Show this help message and exit
    --region=REGION    Amazon region
    --verbose=LEVEL    Write logging messages [default: 30]

    '''
    
    argv = set_defaults(adr_create.__doc__)
    set_logger('adr', int(argv['--verbose']))
    runner_id = adr.create(region_name=argv['--region'])
    return set_runner(runner_id)

    
def adr_launch():
    '''adr_launch : Launch workers

Usage:
    adr launch [<runner>] [options]
    
Positional arguments:
    runner             Runner ID

Options:
    -h, --help         Show this help message and exit
    -n N               Number of workers [default: 1]
    --region=REGION    Amazon region
    --ami=AMI          Amazon Machine Image (AMI)
    --asg=SG           Comma-separated list of Amazon Security Groups
    --akp=KEY          Amazon Key Pair
    --ait=TYPE         Amazon Instance Type
    --verbose=LEVEL    Write logging messages [default: 30]

    '''
    
    argv = set_defaults(adr_launch.__doc__)
    runner_id = get_runner(argv['<runner>'])
    set_logger(runner_id, int(argv['--verbose']))
    workers = adr.launch(runner_id,
                         n=argv['-n'],
                         region_name=argv['--region'],
                         #user=argv['--user'],
                         #password=argv['--password'],
                         #key_filename=argv['--key'],
                         ami=argv['--ami'],
                         asg=argv['--asg'],
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
    --hosts=HOSTS      Comma-separated list of hostnames
    --user=USER        SSH username
    --password=PW      SSH password
    --key=KEY          SSH key filename
    --region=REGION    Amazon region
    --verbose=LEVEL    Write logging messages [default: 30]

    '''
    
    argv = set_defaults(adr_prepare.__doc__)
    runner_id = get_runner(argv['<runner>'])
    set_logger(runner_id, int(argv['--verbose']))
    adr.prepare(runner_id,
                hosts=argv['--hosts'],
                region_name=argv['--region'],
                user=argv['--user'],
                password=argv['--password'],
                key_filename=argv['--key'],
                warn_only=True)


def adr_start():
    '''adr_start : Start workers

Usage:
    adr start [<runner>] [options]
    
Positional arguments:
    runner             Runner ID

Options:
    -h, --help         Show this help message and exit
    --hosts=HOSTS      Comma-separated list of hostnames
    --region=REGION    Amazon region
    --verbose=LEVEL    Write logging messages [default: 30]

    '''
    
    argv = set_defaults(adr_start.__doc__)
    runner_id = get_runner(argv['<runner>'])
    set_logger(runner_id, int(argv['--verbose']))
    adr.start(runner_id,
              region_name=argv['--region'],
              hosts=argv['--hosts'])


def adr_stop():
    '''adr_stop : Stop workers

Usage:
    adr stop [<runner>] [options]
    
Positional arguments:
    runner             Runner ID

Options:
    -h, --help         Show this help message and exit
    --hosts=HOSTS      Comma-separated list of hostnames
    --region=REGION    Amazon region
    --verbose=LEVEL    Write logging messages [default: 30]

    '''
    
    argv = set_defaults(adr_stop.__doc__)
    runner_id = get_runner(argv['<runner>'])
    set_logger(runner_id, int(argv['--verbose']))
    adr.stop(runner_id,
             region_name=argv['--region'],
             hosts=argv['--hosts'])

    
def adr_destroy():
    '''adr_destroy : Destroy runner and workers

Usage:
    adr destroy [<runner>] [options]

Positional arguments:
    runner             Runner ID

Options:
    -h, --help         Show this help message and exit
    --hosts=HOSTS      Comma-separated list of hostnames
    --region=REGION    Amazon region
    --verbose=LEVEL    Write logging messages [default: 30]

    '''
    
    argv = set_defaults(adr_destroy.__doc__)
    runner_id = get_runner(argv['<runner>'])
    set_logger(runner_id, int(argv['--verbose']))
    adr.destroy(runner_id,
                region_name=argv['--region'],
                hosts=argv['--hosts'])
    
    for runner in adr.get_runners():
        if runner != runner_id:
            set_runner(runner)
            break

    
def adr_queue():
    '''adr_queue : Queue batch

Usage:
    adr queue <file>... [<runner>] [options]

Positional arguments:
    file               Input files to queue
    runner             Runner ID

Options:
    -h, --help         Show this help message and exit
    --command=CMD      Shell command
    --verbose=LEVEL    Write logging messages [default: 30]

    '''
    
    argv = set_defaults(adr_queue.__doc__)
    runner_id = get_runner(argv['<runner>'])
    set_logger(runner_id, int(argv['--verbose']))
    return adr.queue(runner_id, argv['<file>'])

    
def adr_process():
    '''adr_process : Process batches from queue

Usage:
    adr process [<runner>] [options]

Positional arguments:
    runner             Runner ID

Options:
    -h, --help         Show this help message and exit
    --workingdir=PATH  Working directory [default: .]
    --region=REGION    Amazon region
    --verbose=LEVEL    Write logging messages [default: 30]

    '''
    
    argv = set_defaults(adr_process.__doc__)
    runner_id = get_runner(argv['<runner>'])
    set_logger(runner_id, int(argv['--verbose']))
    return adr.process(runner_id,
                       workingdir=argv['--workingdir'],
                       region_name=argv['--region'])

    
def adr_download():
    '''adr_download : Download batch results

Usage:
    adr download <path> [<runner>] [options]

Positional arguments:
    path               Download location
    runner             Runner ID

Options:
    -h, --help         Show this help message and exit
    --region=REGION    Amazon region
    --overwrite        Overwrite existing files
    --verbose=LEVEL    Write logging messages [default: 30]

    '''
    
    argv = set_defaults(adr_download.__doc__)
    runner_id = get_runner(argv['<runner>'])
    set_logger(runner_id, int(argv['--verbose']))
    return adr.download(runner_id,
                        argv['<path>'],
                        region_name=argv['--region'],
                        overwrite=argv['--overwrite'] is not None)


def adr_list():
    '''adr_list : List available runners and hosts

Usage:
    adr list [<runner>] [options]

Positional arguments:
    runner             Runner ID

Options:
    -h, --help         Show this help message and exit
    --region=REGION    Amazon region
    --verbose=LEVEL    Write logging messages [default: 30]
    
    '''
    
    argv = set_defaults(adr_list.__doc__)
    runner_id = get_runner(argv['<runner>'])
    set_logger(runner_id, int(argv['--verbose']))
    for worker in adr.get_workers(runner_id, region_name=argv['--region']):
        print worker

    
def adr_set():
    '''adr_set : Set current runner

Usage:
    adr set [<runner>]

Positional arguments:
    runner             Runner ID

Options:
    -h, --help         Show this help message and exit

'''
    
    argv = set_defaults(adr_set.__doc__)
    if argv['<runner>'] is None:
        runner_id = get_runner(argv['<runner>'])
        for runner in adr.get_runners():
            if runner == runner_id:
                print '* {}'.format(runner)
            else:
                print '  {}'.format(runner)
    else:
        return set_runner(argv['<runner>'])


def adr_config():
    '''adr_config : Configure Amazon Distributed Runner

Usage:
    adr config [options]

Options:
    -h, --help         Show this help message and exit

    '''

    argv = set_defaults(adr_config.__doc__)
    config.wizard()

    
### HELPER ###########################################################

def get_runner(runner_id):
    if runner_id is None:
        runner_id = config.load_config('runner')

    if runner_id is None:
        raise ValueError('Please specify runner ID')
    
    return runner_id


def set_runner(runner_id):
    if runner_id is not None:
        config.update_config('runner', runner_id)
        
    return runner_id


def set_defaults(docs):

    cfg = config.load_config()
    defaults = {
        '--region' : config.get_item(cfg, ('aws', 'configuration', 'region')),
        '--user' : config.get_item(cfg, ('ssh', 'user')),
        #'--password' : config.get_item(cfg, ('ssh', 'password')),
        '--key' : config.get_item(cfg, ('ssh', 'key_filename')),
        '--ait' : config.get_item(cfg, ('aws', 'configuration', 'instance_type')),
        '--ami' : config.get_item(cfg, ('aws', 'configuration', 'machine_image')),
        '--asg' : ','.join(config.get_item(cfg, ('aws', 'configuration', 'security_groups'))),
        '--akp' : config.get_item(cfg, ('aws', 'configuration', 'key_pair')),
        '--command' : config.get_item(cfg, ('command', 'command')),
    }

    for k, v in defaults.iteritems():
        if v:
            docs = re.sub(r'^(\s*{}=[A-Z]+\s+.+)\s*$'.format(k),
                          r'\1 [default: {}]'.format(v),
                          docs, flags=re.MULTILINE)

    argv = docopt.docopt(docs)

    # split lists
    for k in ['--hosts', '--asg']:
        if argv.has_key(k):
            if argv[k]:
                argv[k] = argv[k].split(',')

    return argv


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

