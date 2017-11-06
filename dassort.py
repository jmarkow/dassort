import os, json, pprint, yaml, re, itertools, click, time, sys

@click.command()
@click.option('--source','-s', type=click.Path(exists=True), envvar='DASSORT_SOURCE', default=os.getcwd())
@click.option('--destination','-d', type=str, envvar='DASSORT_DESTINATION', default=os.path.join(os.getcwd(),'tmp'))
@click.option('--wait-time','-w', type=float, default=30)
@click.option('--dry-run', type=bool, is_flag=True)
@click.option('--copy-protocol','-p', type=str, default='scp')
@click.option('--delete', type=bool, is_flag=True)
@click.option('--remote-host','-r',type=str, envvar='DASSORT_HOST', default='transfer.rc.hms.harvard.edu')
@click.option('--cmd-host','-c',type=str, envvar='DASSORT_CMDHOST', default='o2.hms.harvard.edu')
@click.option('--remote-user','-u',type=str, envvar='DASSORT_USER', default='johanedoe')
def dassort(source, destination, wait_time, dry_run, copy_protocol, delete, remote_host, cmd_host, remote_user):
    # up front make sure we have a dassort.yaml file in the
    # source directory, otherwise we don't have much to work with!

    config_yaml=read_config(os.path.join(source,'dassort.yaml'))

    # map out the keys for the path builder

    base_dict={
            'keys':config_yaml['keys'],
            'map':config_yaml['map'],
            'value':[],
            'path':{
                    'path_string':config_yaml['path'],
                    're':{'root':destination}
                },
            'command':config_yaml['command'],
            }

    remote_options={
            'user':remote_user,
            'host':remote_host,
            'cmd_host':cmd_host
        }

    if 'destination' in config_yaml:
        base_dict['path']['re']['root']=config_yaml['destination']
    # enter the main loop to watch directories


    while True:
        try:

            # gather all json files, and now figure out which files are associated with which json files

            listing=[os.path.join(source,f) for f in os.listdir(source) if os.path.isfile(f)]
            listing_json=[f for f in listing if f.endswith('.json')]
            listing_dirs_tmp=[os.path.join(source,f) for f in os.listdir(source) if os.path.isdir(f)]
            listing_dirs=[]

            # each json file becomes a key, find any associated files...
            # if any sub directories have json files, let 'er rip

            for dir in listing_dirs_tmp:
                dir_listing=os.listdir(dir)
                dir_json=[f for f in dir_listing if f.endswith('.json')]
                if len(dir_json)>0:
                    listing_dirs.append(dir)

            proc_loop(listing=listing_dirs+listing_json,base_dict=base_dict,
                      copy_protocol=copy_protocol,dry_run=dry_run,delete=delete,
                      remote_options=remote_options)
            print('Sleeping for '+str(wait_time)+' seconds')
            #TODO: exponential back off policy?
            time.sleep(wait_time)
        except KeyboardInterrupt:
            print('Quitting...')
            break
        except Exception as e:
            #raise Exception("The code is buggy: %s" % (e, sys.exc_info()[2]))
            raise



# good idea from https://stackoverflow.com/questions/9807634/\
# find-all-occurrences-of-a-key-in-nested-python-dictionaries-and-lists
def find_key(key, var):
    if hasattr(var,'items'):
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


def read_config(file):
    with open(file) as config_yaml:
        base_yaml=yaml.safe_load(config_yaml)

    # with config loaded, make sure we have the keys that we nede

    config = {
            'keys':[],
            'map':[],
            'path':None,
            'destination':None,
            'command':{
                    'exts':[],
                    'run':None
                }
        }

    tree_yaml=base_yaml['dassort']
    map_json=tree_yaml['json']
    command=tree_yaml['command']
    config=merge_dicts(config,map_json)
    config=merge_dicts(config,tree_yaml)

    return config

def merge_dicts(dict1, dict2):

    merge_dict=dict1

    for key,value in dict1.items():
        if key in dict2:
            merge_dict[key]=dict2[key]

    return merge_dict

def build_path(key_dict, path_string):
    for key,value in key_dict.items():
        if value:
            path_string=re.sub('\$\{'+key+'\}',value,path_string)
    return path_string

def proc_loop(listing,base_dict,copy_protocol,dry_run,delete,remote_options):
    for proc in listing:
        print('Processing '+proc)
        sz=os.path.getsize(proc)

        print('Current size '+str(sz)+' bytes')
        time.sleep(5)
        sz2=os.path.getsize(proc)
        print('New size '+str(sz2)+' bytes')

        if sz2>sz:
            print('Size changed, moving on...')
            continue

        if os.path.isdir(proc):
            isdir=True
            tmp_listing=os.listdir(proc)
            tmp_json=[os.path.join(proc,f) for f in tmp_listing if f.endswith('.json')]
            json_file=tmp_json[0]
            listing_manifest=[os.path.join(proc,f) for f in tmp_listing if os.path.isfile(os.path.join(proc,f))]
        else:
            isdir=False
            json_file=proc
            filename=os.path.splitext(os.path.basename(proc))[0]
            dirname=os.path.dirname(proc)
            listing_manifest=[os.path.join(dirname,f) for f in os.listdir(dirname) if f.startswith(filename)]

        print('Found json file '+json_file)

        with open(json_file) as open_file:
            dict_json=json.load(open_file)

        if 'destination' in dict_json:
            base_dict['path']['re']['root']=dict_json['destination']

        # if it's a directory the manifest is the contents of the directory, if it's not the manifest
        # simply matches filenames

        print('Manifest ['+','.join(listing_manifest)+']')
        generators=[]

        for m in base_dict['map']:
            base_dict['path']['re'][m]=None

        for k,v in zip(base_dict['keys'],base_dict['map']):
            generators=find_key(k,dict_json)
            base_dict['path']['re'][v]=next(generators,base_dict['path']['re'][v])

        # build a path
        new_path=build_path(base_dict['path']['re'],base_dict['path']['path_string'])
        # check for command triggers

        print('Sending manifest to '+new_path)

        # aiight dawg, one trigger per manifest?

        for f in listing_manifest:
            if copy_protocol=='scp':
                # dir check
                dir_cmd="ssh %s@%s 'mkdir -p %s'" % (remote_options['user'],remote_options['host'],new_path)
                cp_cmd="scp %s %s@%s:%s" % (f,remote_options['user'],remote_options['host'],new_path)
            elif copy_protocol=='rsync':
                raise NotImplementedError
            else:
                raise NotImplementedError

            print('Chk command:  '+dir_cmd)
            print('Copy command: '+cp_cmd)

            if not dry_run:
                status=os.system(dir_cmd)
                if status==0:
                    print('Directory creation/check succesful, copying...')
                    status=os.system(cp_cmd)
                    if status==0 and delete:
                        print('Copy succeeded, deleting file')
                        os.remove(os.path.join(new_path,f))
                    elif status==0:
                        print('Copy SUCCESS, continuing')
                    else:
                        print('Copy FAILED, continuing')
                        continue
            elif dry_run and delete:
                print('Would delete: '+os.path.join(new_path,f))

        issue_options={
            'user':'',
            'host':'',
            'cmd_host':'',
            'path':''
        }

        for ext,cmd in zip(base_dict['command']['exts'],base_dict['command']['run']):
            triggers=[f for f in listing_manifest if f.endswith(ext)]
            if triggers and not dry_run and not delete:
                raise NameError("Delete option must be turned on, otherwise triggers will repeat")
            elif triggers and not dry_run:
                issue_options['path']=os.path.join(new_path,os.path.basename(triggers[0]))
                issue_options=merge_dicts(issue_options,remote_options)
                issue_cmd=build_path(issue_options,cmd)
                print('Issuing command '+issue_cmd)
                status=os.system(issue_cmd)
                if status==0:
                    print('Command SUCCESS')
                else:
                    print('Command FAIL')
            elif triggers:
                issue_options['path']=os.path.join(new_path,os.path.basename(triggers[0]))
                issue_options=merge_dicts(issue_options,remote_options)
                issue_cmd=build_path(issue_options,cmd)
                print('Would issue command '+issue_cmd)




if __name__ == "__main__":
    dassort()
