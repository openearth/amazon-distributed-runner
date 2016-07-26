import os
import json
import configparser


AWS_CREDENTIALS_FILE = os.path.expanduser('~/.aws/credentials')
AWS_CONFIG_FILE = os.path.expanduser('~/.aws/config')

JSON_INDENT = 4
JSON_FILE = os.path.expanduser('~/.aws/adr.json')
JSON_DEFAULT = {
    'runner' : '',
    'aws' : {
        'credentials' : {
            'access_key_id' : '',
            'secret_access_key' : '',
        },
        'configuration' : {
            'region' : '',
            'machine_instance' : '',
            'instance_type' : '',
            'security_groups' : [],
            'key_pair' : '',
            'output' : 'json',
        },
    },
    'ssh' : {
        'user' : 'ubuntu',
        'password' : '',
        'key_filename' : '',
    },
    'command' : {
        'command' : '',
        'preprocessing' : '',
        'postprocessing' : '',
    },
}


def load_config(*keys):

    cfg = JSON_DEFAULT.copy()
    
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, 'r') as fp:
            cfg.update(json.load(fp))

    return get_item(cfg, keys)


def update_config(*keys):

    if len(keys) == 0:
        return
    
    cfg = load_config()
    cfg = set_item(cfg, keys[:-1], keys[-1])
    write_config(cfg)
    

def write_config(cfg):

    with open(JSON_FILE, 'w') as fp:
        json.dump(cfg, fp, indent=JSON_INDENT)

    os.chmod(JSON_FILE, 0600)


def write_aws_config(cfg):

    ini = configparser.ConfigParser()
    ini['default'] = {'aws_{}'.format(k):v for k, v in cfg['aws']['credentials'].iteritems()}

    with open(AWS_CREDENTIALS_FILE, 'w') as fp:
        ini.write(fp)
                         
    os.chmod(AWS_CREDENTIALS_FILE, 0600)

    ini = configparser.ConfigParser()
    ini['default'] = {k:','.join(v) if type(v) is list else v for k, v in cfg['aws']['configuration'].iteritems()}

    with open(AWS_CONFIG_FILE, 'w') as fp:
        ini.write(fp)

    os.chmod(AWS_CONFIG_FILE, 0600)

    
def wizard():

    cfg = load_config()
    cfg = ask_question(cfg, ('aws', 'credentials', 'access_key_id'), 'AWS Access Key ID', masked=True)
    cfg = ask_question(cfg, ('aws', 'credentials', 'secret_access_key'), 'AWS Secret Access Key', masked=True)
    cfg = ask_question(cfg, ('ssh', 'user'), 'Default SSH user')
    cfg = ask_question(cfg, ('ssh', 'password'), 'Default SSH password', masked=True)
    cfg = ask_question(cfg, ('ssh', 'key_filename'), 'Default SSH key file')
    cfg = ask_question(cfg, ('aws', 'configuration', 'region'), 'Default region name')
    cfg = ask_question(cfg, ('aws', 'configuration', 'instance_type'), 'Default instance type')
    cfg = ask_question(cfg, ('aws', 'configuration', 'machine_instance'), 'Default machine instance')
    cfg = ask_question(cfg, ('aws', 'configuration', 'security_groups'), 'Default security groups', split=True)
    cfg = ask_question(cfg, ('aws', 'configuration', 'key_pair'), 'Default key pair')
    cfg = ask_question(cfg, ('command', 'command'), 'Default command')
    cfg = ask_question(cfg, ('command', 'preprocessing'), 'Default preprocessing')
    cfg = ask_question(cfg, ('command', 'postprocessing'), 'Default postprocessing')
    cfg = ask_question(cfg, ('aws', 'configuration', 'output'), 'Default output format')
    write_config(cfg)

    if raw_input('Also write AWSCLI config [Y/n]? ') in ['y', 'Y', '']:
        write_aws_config(cfg)


def ask_question(cfg, keys, display, masked=False, split=False):

    val = get_item(cfg, keys)
    val = disp_item(val, masked=masked)
    dsp = '{} [{}]: '.format(display, val)
    
    i = raw_input(dsp)
    if len(i) > 0:
        if split:
            i = [ii.strip() for ii in i.split(',')]
        cfg = set_item(cfg, keys, i)

    return cfg


def get_item(cfg, keys):
    for k in keys:
        if cfg.has_key(k):
            cfg = cfg[k]
        else:
            return None

    return cfg


def set_item(cfg, keys, val):
    if len(keys) > 0:
        k = keys[0]
        if cfg.has_key(k):
            cfg[k] = set_item(cfg[k], keys[1:], val)
    else:
        cfg = val
        
    return cfg


def disp_item(val, masked=False):

    if type(val) is list:
        val = ','.join(val)
        
    if masked:
        n = len(val)
        n1 = int(.8 * n)
        n2 = n - n1
        val = '{}{}'.format('*' * n1, val[-n2:])

    return val
        
