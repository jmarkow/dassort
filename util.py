import json
import yaml
import re
import logging
import os
import time
from itertools import cycle


def find_key(key, var):
    """Finds all occurrences of a key in a nested dictionary, useful for gobbling up
    stuff from json files.

    All credit due to https://stackoverflow.com/questions/9807634/\
    find-all-occurrences-of-a-key-in-nested-python-dictionaries-and-lists
    """
    if hasattr(var, 'items'):
        for k, v in var.items():
            if k == key:
                yield v
            if isinstance(v, dict):
                for result in find_key(key, v):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in find_key(key, d):
                        yield result


def read_config(file, destination=None, user=None, host=None, cmd_host=None, copy_protocol=None):
    """Simple yaml reader to parse config files

    Args:
        file: the yaml file to read the configuration from
    Returns:
        config: a dictionary with the j keys, the path variable they map to, and other bells and whistles...

    """
    with open(file) as config_yaml:
        base_yaml = yaml.safe_load(config_yaml)

    # with config loaded, make sure we have the keys that we need

    base_config = {
        'keys': [],
        'map': [],
        'default': [],
        'path': None,
        'destination': destination,
        'command': {
            'exts': [],
            'run': None
        }
    }

    router_config = {
        'key': [],
        'files': [],
        'filter': None,
        'invert': None,
        'lowercase': None,
        'exact': None
    }

    remote_config = {
        'user': user,
        'host': host,
        'copy_protocol': copy_protocol,
        'cmd_host': cmd_host,
    }

    if 'dassort' in base_yaml.keys() and 'remote' in base_yaml.keys():
        tree_yaml = base_yaml['dassort']
        map_json = tree_yaml['json']
        base_config = merge_dicts(base_config, map_json)
        base_config = merge_dicts(base_config, tree_yaml)
        remote_yaml = base_yaml['remote']
        remote_config = merge_dicts(remote_config, remote_yaml)
        router_config = None
    elif 'dassort' in base_yaml.keys():
        tree_yaml = base_yaml['dassort']
        map_json = tree_yaml['json']
        base_config = merge_dicts(base_config, map_json)
        base_config = merge_dicts(base_config, tree_yaml)
        remote_config = None
        router_config = None
    elif 'router' in base_yaml.keys():
        tree_yaml = base_yaml['router']
        router_config = merge_dicts(router_config, tree_yaml)
        # all router items should be iterables
        for k, v in router_config.items():
            if type(v) is not list:
                router_config[k] = [v]
        base_config = None
        remote_config = None

    # reformat base configuration

    if base_config is not None:
        base_config = {
            'keys': base_config['keys'],
            'map': base_config['map'],
            'default': base_config['default'],
            'value': [],
            'path': {
                'path_string': base_config['path'],
                're': {'root': base_config['destination']}
            },
            'command': base_config['command'],
        }

    return base_config, remote_config, router_config


def merge_dicts(dict1, dict2):
    """Merge dictionary 2 values into dictionary 1, contingent on dictionary 1 containing
    a given key.

    Args:
        dict1: source dictionary
        dict2: merge dictionary
    Returns:
        merge_dict: dict2 merged into dict1
    """
    merge_dict = dict1

    for key, value in dict1.items():
        if key in dict2:
            merge_dict[key] = dict2[key]

    return merge_dict


def build_path(key_dict, path_string):
    """Takes our path string and replaces variables surrounded by braces and prefixed by $
    with a particular value in a key dictionary

    Args:
        key_dict: dictionary where each key, value pair corresponds to a variable and its value
        path_string: path string that specifies how to build our target path_string
    Returns:
        path_string: new path to use

    For example, if the path_string is ${root}/${subject} and key_dict is {'root':'cooldrive','subject':'15781'}
    the path_string is converted to cooldrive/15781
    """
    for key, value in key_dict.items():
        path_string = re.sub('\$\{' + key + '\}', value, path_string)

    return path_string


def get_listing_manifest(proc):
    """Gets the files to ship off with a corresponding json file. If the json file lives in a sub-folder,
    all files in the folder become part of the manifest, if it does not, then all files with a matching filename
    become part of the manifest.

    Args:
        proc: File or directory to process
    Returns:
        listing_manifest: Files to process with json file
        json_file: Json file associated with the manifest

    """
    if os.path.isdir(proc):
        isdir = True
        tmp_listing = os.listdir(proc)
        tmp_json = [os.path.join(proc, f)
                    for f in tmp_listing if f.endswith('.json')]
        json_file = tmp_json[0]
        listing_manifest = [os.path.join(
            proc, f) for f in tmp_listing if os.path.isfile(os.path.join(proc, f))]
    else:
        isdir = False
        json_file = proc
        filename = os.path.splitext(os.path.basename(proc))[0]
        dirname = os.path.dirname(proc)
        listing_manifest = [os.path.join(dirname, f) for f in os.listdir(
            dirname) if f.startswith(filename)]

    return listing_manifest, json_file


def parse_router(router, dirs, files):

    router_status = []
    router_re = []
    for filter, exact in zip(router['filter'], cycle(router['exact'])):
        if exact:
            router_re.append(r'\b{}\b'.format(filter))
        else:
            router_re.append(r'{}'.format(filter))

    # first search directories
    for jsons in dirs:
        js_data = []
        for js in jsons:
            with open(js, 'r') as j:
                js_data.append(json.load(j))
        dir_status = []
        for filter, key, lowercase, invert in zip(router_re,
                                                  cycle(router['key']),
                                                  cycle(router['lowercase']),
                                                  cycle(router['invert'])):
            if lowercase:
                hits = [re.search(filter, j[key], re.IGNORECASE) is not None for j in js_data]
            else:
                hits = [re.search(filter, j[key]) is not None for j in js_data]

            if invert:
                dir_status.append(not any(hits))
            else:
                dir_status.append(any(hits))

        try:
            router_status.append(dir_status.index(True))
        except ValueError:
            router_status.append(None)

    # then search files
    for js in files:

        with open(js, 'r') as j:
            js_data = json.load(j)

        if js_data is None:
            continue

        file_status = []
        for filter, key, lowercase, invert in zip(router_re,
                                                  cycle(router['key']),
                                                  cycle(router['lowercase']),
                                                  cycle(router['invert'])):

            if lowercase:
                hit = re.search(filter, js_data[key], re.IGNORECASE)
            else:
                hit = re.search(filter, js_data[key])

            if invert:
                hit = not hit

            file_status.append(hit)
        try:
            router_status.append(file_status.index(True))
        except ValueError:
            router_status.append(None)

    return router_status


def proc_loop(listing, base_dict, dry_run, delete, remote_options):
    """Main processing loop

    """
    proc_count = 0
    for proc in listing:

        use_dict = base_dict

        logging.info('Processing ' + proc)
        sz = os.path.getsize(proc)

        # loop through manifest, make sure the files are not growing...

        listing_manifest, json_file = get_listing_manifest(proc=proc)

        # changed from <= 1 to < 1 to account for metadata.json getting orphaned...
        if len(listing_manifest) < 1:
            logging.info(
                'Manifest empty, continuing...(maybe files still copying?)')
            continue

        logging.info('Getting file sizes for manifest')
        listing_sz = {f: os.path.getsize(f) for f in listing_manifest}
        time.sleep(10)
        listing_manifest, json_file = get_listing_manifest(proc=proc)
        logging.info('Checking file sizes again')
        listing_sz2 = {f: os.path.getsize(f) for f in listing_manifest}

        if listing_sz != listing_sz2:
            logging.info(
                'A file size changed or a new file was added, continuing...')
            continue

        logging.info('Found json file ' + json_file)

        with open(json_file) as open_file:
            dict_json = json.load(open_file)

        if 'destination' in dict_json:
            use_dict['path']['re']['root'] = dict_json['destination']

        # if it's a directory the manifest is the contents of the directory, if it's not the manifest
        # simply matches filenames

        logging.info('Manifest [' + ','.join(listing_manifest) + ']')
        generators = []

        for m, d in zip(use_dict['map'], use_dict['default']):
            use_dict['path']['re'][m] = d

        for k, v in zip(use_dict['keys'], cycle(use_dict['map'])):
            generators = find_key(k, dict_json)
            use_dict['path']['re'][v] = next(
                generators, use_dict['path']['re'][v])

        # sub folder is a special key to copy over the appropriate sub-folder

        if os.path.isdir(proc):
            use_dict['path']['re']['sub_folder'] = os.path.basename(
                os.path.normpath(proc)) + '/'
        else:
            use_dict['path']['re']['sub_folder'] = ''

        # build a path
        new_path = build_path(
            use_dict['path']['re'], use_dict['path']['path_string'])
        # check for command triggers

        logging.info('Sending manifest to ' + new_path)

        # aiight dawg, one trigger per manifest?

        for f in listing_manifest:
            if remote_options['copy_protocol'] == 'scp':
                # dir check
                dir_cmd = "ssh %s@%s 'mkdir -p \"%s\"'" % (
                    remote_options['user'], remote_options['host'], new_path)
                cp_cmd = "scp \"%s\" %s@%s:'\"%s\"'" % (
                    f, remote_options['user'], remote_options['host'], new_path)
            elif remote_options['copy_protocol'] == 'nocopy':
                dir_cmd = ''
                cp_cmd = ''
            elif remote_options['copy_protocol'] == 'rsync':
                raise NotImplementedError
            else:
                raise NotImplementedError

            logging.info('Chk command:  ' + dir_cmd)
            logging.info('Copy command: ' + cp_cmd)

            if not dry_run:
                status = os.system(dir_cmd)
                if status == 0:
                    logging.info(
                        'Directory creation/check succesful, copying...')
                    status = os.system(cp_cmd)
                    if status == 0 and delete:
                        logging.info('Copy succeeded, deleting file')
                        proc_count += 1
                        os.remove(os.path.join(new_path, f))
                    elif status == 0:
                        logging.info('Copy SUCCESS, continuing')
                        proc_count += 1
                    else:
                        logging.info('Copy FAILED, continuing')
                        continue
            elif dry_run and delete:
                logging.info('Would delete: ' + os.path.join(new_path, f))

        issue_options = {
            'user': '',
            'host': '',
            'cmd_host': '',
            'path': ''
        }

        for ext, cmd in zip(use_dict['command']['exts'], cycle(use_dict['command']['run'])):
            triggers = [f for f in listing_manifest if f.endswith(ext)]
            if triggers and not dry_run and not delete:
                raise NameError(
                    "Delete option must be turned on, otherwise triggers will repeat")
            elif triggers and remote_options['copy_protocol'] == 'nocopy':
                logging.info('nocopy, doing nothing')
            elif triggers and not dry_run:
                issue_options['path'] = os.path.join(
                    new_path, os.path.basename(triggers[0]))
                issue_options = merge_dicts(issue_options, remote_options)
                issue_cmd = build_path(issue_options, cmd)
                logging.info('Issuing command ' + issue_cmd)
                status = os.system(issue_cmd)
                if status == 0:
                    logging.info('Command SUCCESS')
                else:
                    logging.info('Command FAIL')
            elif triggers:
                issue_options['path'] = os.path.join(
                    new_path, os.path.basename(triggers[0]))
                issue_options = merge_dicts(issue_options, remote_options)
                issue_cmd = build_path(issue_options, cmd)
                logging.info('Would issue command ' + issue_cmd)

    return proc_count
