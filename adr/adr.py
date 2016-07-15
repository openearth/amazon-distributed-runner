import os
import re
import time
import uuid
import glob
import boto3
import zipfile
import logging
import subprocess


logger = logging.getLogger(__file__)


def create_queue(sqs, runner_id):
    
    sqs.create_queue(QueueName=runner_id)
    
    logger.info('Created SQS queue "{}".'.format(runner_id))
    
    
def create_bucket(s3, runner_id, ACL='private', location='eu-central-1'):
    
    s3.create_bucket(ACL=ACL,
                     Bucket=runner_id,
                     CreateBucketConfiguration={'LocationConstraint': location})
    
    logger.info('Created S3 bucket "{}".'.format(runner_id))
    
    
def create_runner():

    s3 = boto3.resource('s3')
    sqs = boto3.resource('sqs')
    runner_id = str(uuid.uuid4())
    
    create_queue(sqs, runner_id)
    create_bucket(s3, runner_id)
    
    return runner_id


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
    
    
def queue_batch(runner_id, pattern, command='./aeolis.sh {}',
                preprocessing=None, postprocessing=None, store_patterns=['\.nc$']):
    
    s3 = boto3.resource('s3')
    sqs = boto3.resource('sqs')
    
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
            
            return message
            
        time.sleep(delay)


def run_job(runner_id, path):
    
    s3 = boto3.resource('s3')
    sqs = boto3.resource('sqs')

    # read message
    message = get_job(sqs, runner_id)
    if message is None:
        return False
    runner_id = message.message_attributes['Runner']['StringValue']
    batch_id = message.message_attributes['Batch']['StringValue']
    cmd = message.message_attributes['Command']['StringValue']
    
    # download data
    batchpath = os.path.join(path, batch_id)
    if not os.path.exists(batchpath):
        download_batch(s3, runner_id, batch_id, path)
    
    # run model
    subprocess.call(cmd, cwd=batchpath, shell=True)
    
    # store data
    if message.message_attributes.has_key('Store'):
        store_patterns = message.message_attributes['Store']['StringValue'].split('|')
        for root, dirs, files in os.walk(os.path.join(path, batch_id)):
            for fname in files:
                if any([re.search(p, fname) for p in store_patterns]):
                    key = '{}/{}'.format(batch_id, fname)
                    s3.Object(runner_id, key).upload_file(os.path.join(root, fname))

    return True
