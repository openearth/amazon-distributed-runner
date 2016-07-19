import os
import re
import time
import uuid
import glob
import boto3
import zipfile
import logging
import subprocess
import fabric.api
import fabfile


REGION_NAME = 'eu-central-1b'


logger = logging.getLogger(__file__)


## CREATE ############################################################

def create():

    s3 = boto3.resource('s3', region_name=REGION_NAME)
    sqs = boto3.resource('sqs', region_name=REGION_NAME)
    runner_id = str(uuid.uuid4())
    
    create_queue(sqs, runner_id)
    create_bucket(s3, runner_id)
    
    return runner_id


def create_queue(sqs, runner_id):
    
    sqs.create_queue(QueueName=runner_id)
    
    logger.info('Created SQS queue "{}".'.format(runner_id))
    
    
def create_bucket(s3, runner_id, ACL='private', location='eu-central-1'):
    
    s3.create_bucket(ACL=ACL,
                     Bucket=runner_id,
                     CreateBucketConfiguration={'LocationConstraint': location})
    
    logger.info('Created S3 bucket "{}".'.format(runner_id))
    

## LAUNCH ############################################################

def launch(runner_id, n, user='ubuntu', password=None, key_filename=None, **kwargs):

    ec2 = boto3.resource('ec2', region_name=REGION_NAME)
    s3 = boto3.resource('s3', region_name=REGION_NAME)

    #workers = launch_workers(ec2, n=n, **kwargs)
    workers = ['52.59.70.42']

    # register workers
    for worker in workers:
        key = '_workers/{}'.format(worker)
        s3.Object(runner_id, key).put(Body='')

    # install and execute queue processor
    with fabric.api.settings(user=user, password=password, key_filename=key_filename,
                             hosts=workers):
        #fabric.api.execute(fabfile.install)
        fabric.api.execute(fabfile.stop)
        fabric.api.execute(fabfile.start, runner_id=runner_id)

    return workers


def launch_workers(ec2, n=1, ami='ami-d09b6ebf', asg=['sg-13d17c7b'], akp='Amazon AeoLiS Test Key', ait='m3.medium'):
    
    instances = ec2.create_instances(ImageId=ami,
                                     MinCount=int(n),
                                     MaxCount=int(n),
                                     InstanceType=ait,
                                     KeyName=akp,
                                     SecurityGroupIds=asg)
    
    # wait until all instances are available
    hosts = []
    for instance in instances:
        instance.wait_until_running()
        instance.reload()
        hosts.append(instance.public_ip_address)

    return list(set(hosts))
        
## LIST ##############################################################

def list_workers(key='_workers/'):

    sqs = boto3.resource('sqs', region_name=REGION_NAME)
    queues = [os.path.split(q.url)[1] for q in sqs.queues.all()]
    
    s3 = boto3.resource('s3', region_name=REGION_NAME)

    runners = {}
    for bucket in s3.buckets.all():
        if bucket.name in queues:
            runner_id = bucket.name

            # queue and bucket available
            runners[runner_id] = []
            for obj in bucket.objects.filter(Prefix=key):
                if obj.key != key:
                    runners[runner_id].append(os.path.split(obj.key)[1])

    return runners
            

### PROCESS ##########################################################

def process(runner_id):

    print runner_id
    
    
def upload_files(s3, runner_id, batch_id, path, include_patterns=['\.nc$']):
    for root, dirs, files in os.walk(os.path.join(path, batch_id)):
        for fname in files:
            if any([re.search(p, fname) for p in include_patterns]):
                key = '{}/{}'.format(batch_id, fname)
                fpath = os.path.join(root, fname)
                s3.Object(runner_id, key).upload_file(fpath)

                logger.info('Uploaded "{}" to "{}/" in bucket "{}".'.format(os.path.relpath(fpath, path), batch_id, runner_id))
                

def upload_batch(s3, runner_id, path, exclude_patterns=['\.log$', '\.nc$', '\.pyc$']):
    
    batch_id = str(uuid.uuid4())

    logger.info('Creating batch "{}"...'.format(batch_id))
    
    zfile = '{}.zip'.format(batch_id)
    zpath = os.path.abspath(os.path.join(path, '..', zfile))
    with zipfile.ZipFile(zpath, mode='w', compression=zipfile.ZIP_DEFLATED) as zh:
        
        if os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                for fname in files:

                    abspath = os.path.join(root, fname)

                    # check if any exclude pattern matches
                    if any([re.search(p, abspath) for p in exclude_patterns]):
                        continue

                    relpath = os.path.relpath(abspath, path)
                    zh.write(abspath, os.path.join(batch_id, relpath))
        else:
            zh.write(path, os.path.join(batch_id, os.path.split(path)[1]))

    logger.info('Created "{}".'.format(zpath))
    
    s3.Object(runner_id, zfile).upload_file(zpath)
    
    logger.info('Uploaded "{}" to bucket "{}".'.format(zfile, runner_id))
    
    os.unlink(zpath)
    
    logger.info('Removed "{}".'.format(zpath))
    
    return batch_id
            
            
def download_batch(s3, runner_id, batch_id, path):
    
    zfile = '{}.zip'.format(batch_id)
    zpath = os.path.join(path, zfile)
    
    s3.Object(runner_id, zfile).download_file(zpath)
    
    logger.info('Downloaded "{}" from bucket "{}".'.format(zfile, runner_id))
    
    if zipfile.is_zipfile(zpath):
        with zipfile.ZipFile(zpath, mode='r') as zh:
            zh.extractall(path)
            
        logger.info('Extracted "{}".'.format(zpath))
            
    os.unlink(zpath)
    
    logger.info('Removed "{}".'.format(zpath))


def find_root(files):
    parts = [f.split(os.path.sep) for f in files]
    ix = [len(set(x))<=1 for x in zip(*parts)].index(False)
    root = os.path.sep.join(parts[0][:ix])
    
    logger.info('Determined root directory: "{}".'.format(root))
    
    return root


def parse_message(message):
    parsed = {}
    for k, v in message.message_attributes.iteritems():
        parsed[k] = v['StringValue']
    return parsed


def queue_job(sqs, runner_id, batch_id, command,
              store_patterns=None, preprocessing=None, postprocessing=None):
    
    queue = sqs.get_queue_by_name(QueueName=runner_id)

    attributes = {
        'Runner' : {
            'StringValue' : runner_id,
            'DataType' : 'String',
        },
        'Batch' : {
            'StringValue' : batch_id,
            'DataType' : 'String',
        },
        'Command' : {
            'StringValue' : command,
            'DataType' : 'String',
        },
    }
    
    if preprocessing:
        attributes['PreProcessing'] = {
            'StringValue' : preprocessing,
            'DataType' : 'String',
        }

    if postprocessing:
        attributes['PostProcessing'] = {
            'StringValue' : postprocessing,
            'DataType' : 'String',
        }

    if store_patterns:
        attributes['Store'] = {
            'StringValue' : '|'.join(store_patterns),
            'DataType' : 'String',
        }
        
    stats = queue.send_message(
        MessageBody='execution',
        MessageAttributes=attributes,
    )
    
    logger.info('Queued job "{}" from batch "{}" for runner "{}".'.format(stats['MessageId'], batch_id, runner_id))
    
    
def queue_batch(runner_id, pattern, command='./run.sh {}',
                preprocessing=None, postprocessing=None, store_patterns=['\.nc$']):
    
    s3 = boto3.resource('s3', region_name=REGION_NAME)
    sqs = boto3.resource('sqs', region_name=REGION_NAME)
    
    files = glob.glob(pattern)
    root = find_root(files)

    batch_id = upload_batch(s3, runner_id, root)
    
    for fpath in files:
        fpath = os.path.relpath(fpath, root)
        queue_job(sqs, runner_id, batch_id, command=command.format(fpath),
                  preprocessing=preprocessing, postprocessing=postprocessing, store_patterns=store_patterns)


def get_job(sqs, runner_id, delay=10, retry=30):
    
    queue = sqs.get_queue_by_name(QueueName=runner_id)
    
    messages = []
    for i in range(retry):
        
        logger.info('Polling queue "{}" ({}/{})...'.format(runner_id, i, retry))
        
        messages = queue.receive_messages(
            MessageAttributeNames=['*'],
            MaxNumberOfMessages=1,
        )
        
        messages = [m for m in messages if m.body == 'execution']
        
        if len(messages) > 0:
            message = messages[0]
            message.delete()
            
            logger.info('Received message from queue "{}".'.format(runner_id))
            
            return parse_message(message)
            
        time.sleep(delay)


def run_job(runner_id, path):
    
    s3 = boto3.resource('s3', region_name=REGION_NAME)
    sqs = boto3.resource('sqs', region_name=REGION_NAME)

    # read message
    message = get_job(sqs, runner_id)
    if message is None:
        return False
    
    # download data
    batchpath = os.path.join(path, batch_id)
    if not os.path.exists(batchpath):
        download_batch(s3, runner_id, batch_id, path)
    
    # run model
    subprocess.call(cmd, cwd=batchpath, shell=True)
    
    # store data
    if message.has_key('Store'):
        store_patterns = message['Store'].split('|')
        upload_files(s3, runner_id, batch_id, path, include_patterns=store_patterns)

    return True