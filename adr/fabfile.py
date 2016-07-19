import os
import tempfile
from fabric.api import *
from fabric.contrib.files import *


def runv(cmd, env='~/.envs/adr', socket=None):
     cmd = 'source {}/bin/activate && {}'.format(env, cmd)
     if socket is not None:
          fp = tempfile.NamedTemporaryFile(suffix='.sh', delete=False)
          fp.write('#!/bin/bash\n\n')
          fp.write('{}\n'.format(cmd))
          fp.close()
          put(fp.name, '~/adr.sh')
          run('chmod a+x ~/adr.sh')
          run('dtach -n `mktemp -u ~/{0}.XXXX` ~/adr.sh'.format(socket))
          os.unlink(fp.name)
     else:
          return run(cmd)


#@parallel
@task
def stop():

     run('pkill adr', warn_only=True)


#@parallel
@task
def start(runner_id):

     return runv('adr process {}'.format(runner_id), socket='adr')


#@parallel
@task
def install(required_packages=['boto3', 'fabric', 'docopt']):

     # make sure dtach is installed
     if not run('which dtach'):
          sudo('apt-get install dtach')
          
     # make sure virtualenv directory exists
     if not exists('~/.envs'):
          run('mkdir ~/.envs')

     # make sure virtualenv is installed
     if not run('which virtualenv'):
          sudo('pip install virtualenv')

     # make sure virtualenv adr exists
     if not exists('~/.envs/adr'):
          run('virtualenv ~/.envs/adr')
         
     # check for installed packages
     packages = runv('pip freeze')
     packages = re.split('\s+', re.sub('==[\d\.]+', '', packages.lower()))
    
     # install missing packages
     for package in required_packages:
          if package.lower() not in packages:
               runv('pip install {}'.format(package))
              
     # install adr from github.com
     if not contains('~/.ssh/config', 'Host github.com'):
          append('~/.ssh/config', 'Host github.com\n  StrictHostKeyChecking no')

     runv('pip install --upgrade git+git://github.com/openearth/amazon-distributed-runner.git')

     # copy credentials
     if not exists('~/.aws'):
          run('mkdir ~/.aws')

     if os.path.exists(os.path.expanduser('~/.aws/config')):
          put('~/.aws/config', '~/.aws/config')

     if os.path.exists(os.path.expanduser('~/.aws/credentials')):
          put('~/.aws/credentials', '~/.aws/credentials')
