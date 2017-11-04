import os, json, pprint, yaml, re, itertools, click

@click.command()
@click.option('--source','-s', type=click.Path(exists=True), envvar='DASSORT_SOURCE', default=os.getcwd())
@click.option('--destination','-d', type=str, envvar='DASSORT_DESTINATION', default=os.path.join(os.getcwd(),'tmp'))
@click.option('--wait-time','-w', type=float, default=30)
@click.option('--dry-run', type=bool, is_flag=True)
@click.option('--copy-protocol','-p', type=str, default='scp')
def dassort(source, destination, wait_time, dry_run, copy_protocol):
    # up front make sure we have a dassort.yaml file in the
    # source directory, otherwise we don't have much to work with!

    config_yaml=read_config(os.path.join(source,'data-sort.yaml'))

    # this can override defaults, double check everything

    listing=[os.path.join(source,f) for f in os.listdir(source) if os.path.isfile(f)]
    listing_json=[f for f in listing if f.endswith('.json')]

    # each json file becomes a key, find any associated files...

    dict_json={}
    dict_manifest={}

    for file in listing_json:
        print('Found json file '+file)

        with open(file) as json_file:
            dict_json[file]=json.load(json_file)

        basename=os.path.splitext(file)[0]
        listing_manifest=[f for f in listing if f.startswith(basename) and not f==file]
        dict_manifest[file]=listing_manifest

        # build a path



# good idea from https://stackoverflow.com/questions/9807634/\
# find-all-occurrences-of-a-key-in-nested-python-dictionaries-and-lists
def find_key(key, var):
    if hasattr(var,'items'):
        for k, v in var.items():
            if k == key:
                yield v
                if isinstance(v, dict):
                    for result in gen_dict_extract(key, v):
                        yield result
                elif isinstance(v, list):
                    for d in v:
                        for result in gen_dict_extract(key, d):
                            yield result


def read_config(file):
    try:
        with open(file) as config_yaml:
            base_yaml=yaml.safe_load(config_yaml)
    except IOError:
        print('Could not load yaml file '+file)

    # with config loaded, make sure we have the keys that we nede

    config = {
            'keys':[],
            'map':'',
            'path':'',
            'destination':'',
            'command':{
                    'exts':'',
                    'run':''
                }
        }

    try:
        tree_yaml=base_yaml['data-sort']
        map_json=tree_yaml['json']
        command=tree_yaml['command']
        config=merge_dicts(config,map_json)
        config=merge_dicts(config,tree_yaml)
    except Exception:
        print('Could not parse yaml file')

    return config

def merge_dicts(dict1, dict2):

    merge_dict=dict1

    for key,value in dict1.items():
        if key in dict2:
            merge_dict[key]=dict2[key]

    return merge_dict

def build_path(keys, path_string):
    pass

if __name__ == "__main__":
    dassort()
