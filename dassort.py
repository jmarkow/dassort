import os, json, pprint, yaml, re, itertools, click

@click.command()
@click.option('--source','-s', type=click.Path(exists=True), envvar='DASSORT_SOURCE')
@click.option('--destination','-d', type=str, envvar='DASSORT_DESTINATION')
@click.option('--wait-time','w', type=float, default=30)
@click.option('--dry-run', type=bool, is_flag=True)
def dassort(source, destination, wait):
    config_yaml=os.path.join(source,'data-sort.yaml')

    # load the config, decide what our defaults are

    # start point for daemon (use a while loop, raise keyboard exception for ctrl+c)

    # up front make sure we have a dassort.yaml file in the
    # source directory, otherwise we don't have much to work with!

    listing_dir=os.listdir(source)
    listing_json=[os.path.join(source,f) for f in listing_dir if os.path.isfile(f) and f.endswith('.json')]

    # each json file becomes a key, find any associated files...

    dict_json=[]

    for file in listing_json:
        print('Found json file '+file)
        with open(file) as json_file:
            dict_json[file]=json.load(json_file)

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
            'command':{
                    'exts':'',
                    'run':''
                },
            'destination'
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

def merge_dicts(dict1,dict2):

    merge_dict=dict1

    for key,value in dict1.items():
        if key in dict2:
            merge_dict[key]=dict2[key]

    return merge_dict

if __name__ == "__main__":
    dassort()
