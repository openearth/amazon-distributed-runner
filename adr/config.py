import os
import json
import configparser


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
            'security_group' : '',
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

    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, 'r') as fp:
            cfg = json.load(fp)
    else:
        cfg = JSON_DEFAULT.copy()

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


def write_aws_config(cfg):

    ini = configparser.ConfigParser()
    ini['default'] = cfg['aws']['credentials']

    with open(os.path.expanduser('~/.aws/credentials'), 'w') as fp:
        ini.write(fp)
                         
    ini = configparser.ConfigParser()
    ini['default'] = cfg['aws']['configuration']

    with open(os.path.expanduser('~/.aws/config'), 'w') as fp:
        ini.write(fp)


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
    cfg = ask_question(cfg, ('aws', 'configuration', 'security_group'), 'Default security group')
    cfg = ask_question(cfg, ('aws', 'configuration', 'key_pair'), 'Default key pair')
    cfg = ask_question(cfg, ('command', 'command'), 'Default command')
    cfg = ask_question(cfg, ('command', 'preprocessing'), 'Default preprocessing')
    cfg = ask_question(cfg, ('command', 'postprocessing'), 'Default postprocessing')
    cfg = ask_question(cfg, ('aws', 'configuration', 'output'), 'Default output format')
    write_config(cfg)

    if raw_input('Also write AWSCLI config [Y/n]? ') in ['y', 'Y', '']:
        write_aws_config(cfg)


def ask_question(cfg, keys, display, masked=False):

    val = get_item(cfg, keys)

    if masked:
        n1 = int(.8 * len(val))
        n2 = len(val) - n1
        val_dsp = '{}{}'.format('*' * n1, val[-n2:])
    else:
        val_dsp = val
        
    dsp = '{} [{}]: '.format(display, val_dsp)
    i = raw_input(dsp)

    if len(i) > 0:
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
