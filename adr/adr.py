import os
import re
import time
import uuid
import glob
import boto3
import zipfile
import logging
import subprocess
import fabfile
import botocore.exceptions


REGION_NAME = 'eu-central-1'


logger = logging.getLogger(__name__)


### CREATE ###########################################################

def create(region_name=REGION_NAME):

    s3 = boto3.resource('s3', region_name=region_name)
    sqs = boto3.resource('sqs', region_name=region_name)
    runner_id = str(uuid.uuid4())
    
    create_queue(sqs, runner_id)
    create_bucket(s3, runner_id, region_name=region_name)
    
    return runner_id


def create_queue(sqs, runner_id):
    
    sqs.create_queue(QueueName=runner_id)
    
    logger.info('Created SQS queue "{}".'.format(runner_id))
    
    
def create_bucket(s3, runner_id, region_name=REGION_NAME, ACL='private'):
    
    s3.create_bucket(ACL=ACL,
                     Bucket=runner_id,
                     CreateBucketConfiguration={'LocationConstraint': region_name})
    
    logger.info('Created S3 bucket "{}".'.format(runner_id))
    

### LAUNCH ###########################################################

def launch(runner_id, n, region_name=REGION_NAME,
           user='ubuntu', password=None, key_filename=None, **kwargs):

    ec2 = boto3.resource('ec2', region_name=region_name)
    s3 = boto3.resource('s3', region_name=region_name)

    workers = launch_workers(ec2, runner_id, n=n, **kwargs)
    register_workers(s3, runner_id, workers)
    
    # install and execute queue processor
    #prepare_workers(workers, runner_id,
    #                user=user, password=password, key_filename=key_filename)

    return workers


def launch_workers(ec2, runner_id, n=1,
                   ami='ami-d09b6ebf', asg=['sg-13d17c7b'],
                   akp='Amazon AeoLiS Test Key', ait='m3.medium'):
    
    instances = ec2.create_instances(ImageId=ami,
                                     MinCount=int(n),
                                     MaxCount=int(n),
                                     InstanceType=ait,
                                     KeyName=akp,
                                     SecurityGroupIds=asg)
    
    # wait until all instances are available
    hosts = []
    for i, instance in enumerate(instances):
        instance.wait_until_running()

        name = '{}_{}'.format(runner_id[:7], i)
        instance.create_tags(Tags=[{'Key':'Name', 'Value':name},
                                   {'Key':'Runner', 'Value':runner_id}])
        
        instance.reload()
        hosts.append(instance.public_ip_address)

        logger.info('Launched instance "{}"'.format(instance.instance_id))

    return list(set(hosts))


def start(runner_id, region_name=REGION_NAME):

    s3 = boto3.resource('s3', region_name=region_name)
    
    for instance in iterate_workers(runner_id, region_name=region_name):
        if instance.state['Name'] == 'stopped':
            instance.start()

    for instance in iterate_workers(runner_id, region_name=region_name):
        instance.wait_until_running()
        instance.reload()

        logger.info('Started instance "{}"'.format(instance.instance_id))

        # register worker
        register_workers(s3, runner_id, [instance.public_ip_address])


def stop(runner_id, region_name=REGION_NAME):

    s3 = boto3.resource('s3', region_name=region_name)
    
    for instance in iterate_workers(runner_id, region_name=region_name):
        if instance.state['Name'] == 'running':

            # deregister worker
            deregister_workers(s3, runner_id, [instance.public_ip_address])

            instance.stop()
            logger.info('Stopped instance "{}"'.format(instance.instance_id))


def prepare(runner_id, region_name=REGION_NAME,
            user='ubuntu', password=None, key_filename=None,
            warn_only=False, timeout=600):
    
    workers = adr.get_workers(runner_id, region_name=region_name)

    with fabfile.settings(user=user, password=password, key_filename=key_filename,
                          hosts=workers, warn_only=warn_only,
                          command_timeout=timeout, skip_bad_hosts=True):
        fabfile.execute(fabfile.install)
        fabfile.execute(fabfile.stop)
        fabfile.execute(fabfile.start, runner_id=runner_id)


def register_workers(s3, runner_id, workers, key='_workers/'):

    if not isiterable(workers):
        workers = [workers]

    for worker in workers:
        s3.Object(runner_id, ''.join(key, worker)).put(Body='')

        
def deregister_workers(s3, runner_id, workers, key='_workers/'):

    if not isiterable(workers):
        workers = [workers]

    for worker in workers:
        s3.Object(runner_id, ''.join(key, worker)).delete(Body='')


### PROCESS ##########################################################

def process(runner_id, workingdir='.', region_name=REGION_NAME, stop_on_empty=False):
    
    s3 = boto3.resource('s3', region_name=region_name)
    sqs = boto3.resource('sqs', region_name=region_name)

    while True:
        if not process_job(sqs, s3, runner_id, workingdir='.'):
            if stop_on_empty:
                logger.info('No jobs left. Stop')
                break
    

def process_job(sqs, s3, runner_id, workingdir='.'):
    
    # read message
    message = get_job(sqs, runner_id)
    if message is None:
        return False

    batch_id = message['Batch']
    cmd = message['Command']
    
    # download data
    batchpath = os.path.join(workingdir, batch_id)
    if not os.path.exists(batchpath):
        download_batch(s3, runner_id, batch_id, workingdir)
        cache_contents(batchpath)
    
    # run model
    shfile = 'run.sh'
    shpath = os.path.join(batchpath, shfile)
    with open(shpath, 'w') as fp:
        fp.write('#!/bin/bash\n\n')
        if message.has_key('PreProcessing'):
            fp.write('{}\n'.format(message['PreProcessing']))
        fp.write('{}\n'.format(cmd))
        if message.has_key('PostProcessing'):
            fp.write('{}\n'.format(message['PostProcessing']))
    os.chmod(shpath, 0744)
    subprocess.call('./{}'.format(shfile), #dtach -n `mktemp -u ~/aeolis.XXXX` 
                    cwd=batchpath, shell=True)
    
    # store data
    if message.has_key('Store'):
        store_patterns = message['Store'].split('|')
        upload_files(s3, runner_id, batch_id, workingdir,
                     include_patterns=store_patterns)
        restore_contents(batchpath)

    return True


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


def upload_files(s3, runner_id, batch_id, path,
                 include_patterns=['\.nc$'], overwrite=False):

    for root, dirs, files in os.walk(os.path.join(path, batch_id)):
        for fname in files:
            if any([re.search(p, fname) for p in include_patterns]):
                key = '{}/{}'.format(batch_id, fname)
                if not key_exists(s3, runner_id, key) or overwrite:
                    fpath = os.path.join(root, fname)
                    s3.Object(runner_id, key).upload_file(fpath)

                    logger.info('Uploaded "{}" to "{}/" in bucket "{}".'.format(
                        os.path.relpath(fpath, path), batch_id, runner_id))
                

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


def parse_message(message):
    parsed = {}
    for k, v in message.message_attributes.iteritems():
        parsed[k] = v['StringValue']
    return parsed


### QUEUE ############################################################

def queue(runner_id, files, region_name=REGION_NAME, command='aeolis {}',
          preprocessing='source ~/.envs/aeolis/bin/activate', postprocessing=None,
          store_patterns=['\.nc$']):
    
    s3 = boto3.resource('s3', region_name=region_name)
    sqs = boto3.resource('sqs', region_name=region_name)
    
    files = [os.path.abspath(f) for f in files]
    root = find_root(files)

    batch_id = upload_batch(s3, runner_id, root)
    
    for fpath in files:
        fpath = os.path.relpath(fpath, root)
        queue_job(sqs, runner_id, batch_id, command=command.format(fpath),
                  preprocessing=preprocessing, postprocessing=postprocessing,
                  store_patterns=store_patterns)


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
    
    logger.info('Queued job "{}" from batch "{}" for runner "{}".'.format(
        stats['MessageId'], batch_id, runner_id))


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
            

### DOWNLOAD #########################################################

def download(runner_id, path, region_name=REGION_NAME):

    s3 = boto3.resource('s3', region_name=region_name)
    for bucket in s3.buckets.all():
        if bucket.name == runner_id:
            for obj in bucket.objects.all():
                if not obj.key.endswith('.zip') and not obj.key.startswith('_'):
                    fpath, fname = os.path.split(obj.key)
                    downloadpath = os.path.join(path, fpath)
                    if not os.path.exists(downloadpath):
                        os.makedirs(downloadpath)
                    obj.meta.client.download_file(bucket.name, obj.key, os.path.join(downloadpath, fname))
                    logger.info('Downloaded "{}" to "{}"'.format(obj.key, downloadpath))

                                
### DESTROY ##########################################################

def destroy(runner_id, region_name=REGION_NAME):

    # delete queue
    sqs = boto3.resource('sqs', region_name=region_name)
    queue = sqs.get_queue_by_name(QueueName=runner_id)
    queue.delete()
    logger.info('Deleted queue "{}"'.format(runner_id))

    # terminate workers
    for instance in iterate_workers(runner_id, region_name=region_name):
        instance.terminate()
        logger.info('Terminated instance "{}"'.format(instance.instance_id))

    # clean bucket
    s3 = boto3.resource('s3', region_name=region_name)
    for bucket in s3.buckets.all():
        if bucket.name == runner_id:
            for obj in bucket.objects.all():
                if obj.key.endswith('.zip'):
                    obj.delete()
                    logger.info('Deleted key "{}"'.format(obj.key))
                elif obj.key.startswith('_'):
                    obj.delete()
                    logger.info('Deleted key "{}"'.format(obj.key))


### LIST #############################################################

def get_runners(region_name=REGION_NAME):

    sqs = boto3.resource('sqs', region_name=region_name)
    s3 = boto3.resource('s3', region_name=region_name)
    
    queues = [os.path.split(q.url)[1] for q in sqs.queues.all()]
    
    runners = []
    for bucket in s3.buckets.all():
        if bucket.name in queues:
            runners.append(bucket.name)

    return runners

        
def get_workers(runner_id, region_name=REGION_NAME, key='_workers/'):

    s3 = boto3.resource('s3', region_name=region_name)

    workers = []
    for bucket in s3.buckets.all():
        if bucket.name == runner_id:
            for obj in bucket.objects.filter(Prefix=key):
                if obj.key != key:
                    workers.append(os.path.split(obj.key)[1])

    return workers
            

### HELPER ###########################################################

def key_exists(s3, runner_id, key):
    
    try:
        s3.Object(runner_id, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise e

    return True


def find_root(files):
    parts = [f.split(os.path.sep) for f in files]
    ix = [len(set(x))<=1 for x in zip(*parts)].index(False)
    root = os.path.sep.join(parts[0][:ix])
    
    logger.info('Determined root directory: "{}".'.format(root))
    
    return root


def cache_contents(path):
    cachepath = os.path.join(path, '.contents')
    with open(cachepath, 'w') as fp:
        for root, dirs, files in os.walk(path):
            for fname in files:
                fpath = os.path.abspath(os.path.join(root, fname))
                fp.write('{}\n'.format(fpath))

    return cachepath


def restore_contents(path):
    cachepath = os.path.join(path, '.contents')
    if os.path.exists(cachepath):
        with open(cachepath, 'r') as fp:
            cache = [l.strip() for l in fp.readlines()]
        for root, dirs, files in os.walk(path):
            for fname in files:
                fpath = os.path.abspath(os.path.join(root, fname))
                if fpath not in cache:
                    os.unlink(fpath)


def iterate_workers(runner_id, region_name=REGION_NAME):

    ec2 = boto3.resource('ec2', region_name=region_name)
    workers = get_workers(runner_id, region_name=region_name)
    
    for instance in ec2.instances.filter(Filters=[{'Name':'ip-address','Values':workers}]):
        if runner_id in [t['Value'] for t in instance.tags if t['Key'] == 'Runner']:
            yield instance


def isiterable(lst):

    try:
        iterator = iter(list)
    except TypeError:
        return False

    return True
