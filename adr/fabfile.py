from fabric.api import *
from fabric.contrib.files import *


#env.user = 'ubuntu'
#env.key_filename = '/Users/hoonhout/.ssh/amazon/AmazonAeoLiSTestKey.pem'


def runv(cmd, env='~/.envs/adr', socket=None):
     cmd = 'source {}/bin/activate && {}'.format(env, cmd)
     if socket is not None:
          run('echo "{}" > ~/adr.sh'.format(cmd))
          run('chmod a+x ~/adr.sh')
          return run('dtach -n `mktemp -u ~/{0}.XXXX` "~/adr.sh"'.format(socket))
     else:
          return run(cmd)


@parallel
def stop():

     run('pkill adr', warn_only=True)


@parallel
def start(runner_id):

     return runv('adr process {}'.format(runner_id), socket='adr')


@parallel
def install(required_packages=['boto3', 'fabric', 'docopt']):

     # install dtach
     if not run('which dtach'):
          sudo('apt-get install dtach')
          
     # make sure virtualenv directory extists
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

     runv('pip install git+git://github.com/openearth/amazon-distributed-runner.git')

