import os
import click
import time
import sys
import logging
from glob import glob
from util import proc_loop, read_config, parse_router
from copy import deepcopy


@click.command()
@click.option('--source', '-s', type=click.Path(exists=True), envvar='DASSORT_SOURCE', default=os.getcwd())
@click.option('--destination', '-d', type=str, envvar='DASSORT_DESTINATION', default=os.path.join(os.getcwd(), 'tmp'))
@click.option('--wait-time', '-w', type=click.IntRange(2, None), default=2)
@click.option('--max-time', '-m', type=float, default=600)
@click.option('--dry-run', type=bool, is_flag=True)
@click.option('--copy-protocol', '-p', type=str, default='scp')
@click.option('--delete', type=bool, is_flag=True)
@click.option('--remote-host', '-r', type=str, envvar='DASSORT_HOST', default='transfer.rc.hms.harvard.edu')
@click.option('--cmd-host', '-c', type=str, envvar='DASSORT_CMDHOST', default='o2.hms.harvard.edu')
@click.option('--remote-user', '-u', type=str, envvar='DASSORT_USER', default='johanedoe')
def dassort(source, destination, wait_time, max_time, dry_run, copy_protocol, delete, remote_host, cmd_host, remote_user):
    """Main outer loop for watching files

    """

    # up front make sure we have a dassort.yaml file in the
    # source directory, otherwise we don't have much to work with!

    wait_time = float(wait_time)
    ymls = glob(os.path.join(source, '*.yaml'))

    configs = []
    router = None

    remote_defaults = {
        'user': remote_user,
        'host': remote_host,
        'cmd_host': cmd_host
    }

    for yml in ymls:
        (base_config,
         remote_config,
         router_config) = read_config(yml,
                                      destination,
                                      host=remote_host,
                                      user=remote_user,
                                      cmd_host=cmd_host,
                                      copy_protocol=copy_protocol)

        if (router_config is not None
            and len(router_config['key']) > 0
            and len(router_config['files']) > 0):
            router = router_config
        elif base_config is not None and remote_config is None:
            configs.append((os.path.basename(yml), base_config, remote_defaults))
        elif base_config is not None:
            configs.append((os.path.basename(yml), base_config, remote_config))
        else:
            raise RuntimeError('Yaml misspecification')

    if router is not None:
        has_router = True
    else:
        has_router = False

    if len(configs) == 0:
        raise RuntimeError('No configuration file found!')

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
                        format="[%(asctime)s]: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")

    # enter the main loop to watch directories

    sleep_time = deepcopy(wait_time)

    while True:
        try:
            # gather all json files, and now figure out which files are associated with which json files

            listing = [os.path.join(source, f)
                       for f in sorted(os.listdir(source)) if os.path.isfile(f)]
            listing_json = [f for f in listing if f.endswith('.json')]
            listing_dirs_tmp = [os.path.join(source, f) for f
                                in sorted(os.listdir(source)) if os.path.isdir(f)]
            listing_dirs = []
            listing_dirs_json = []
            # each json file becomes a key, find any associated files...
            # if any sub directories have json files, let 'er rip

            for dir in listing_dirs_tmp:
                dir_listing = os.listdir(dir)
                dir_json = [os.path.join(dir, f) for f in dir_listing if f.endswith('.json')]
                if len(dir_json) > 0:
                    listing_dirs.append(dir)
                    listing_dirs_json.append(dir_json)

            listing_total = listing_dirs + listing_json

            if has_router:

                router_status = parse_router(router, listing_dirs_json, listing_json)
                proc_count = 0
                iter_status = set([_ for _ in router_status if _ is not None])
                for status in iter_status:
                    new_listing = [lst for (lst, st) in zip(listing_total, router_status) if st is status]
                    fname = router['files'][status]
                    use_config = [(cfg[1], cfg[2]) for cfg in configs if cfg[0] == fname]

                    if len(use_config) == 0:
                        continue
                    proc_count += proc_loop(listing=new_listing,
                                            base_dict=use_config[0][0],
                                            dry_run=dry_run,
                                            delete=delete,
                                            remote_options=use_config[0][1])
            else:
                proc_count = proc_loop(listing=listing_total,
                                       base_dict=configs[0][1],
                                       dry_run=dry_run,
                                       delete=delete,
                                       remote_options=configs[0][2])

            if proc_count == 0:
                sleep_time *= 2
                sleep_time = min(sleep_time, max_time)
            else:
                sleep_time = deepcopy(wait_time)

            logging.info('Sleeping for ' + str(sleep_time) + ' seconds')
            time.sleep(sleep_time)

        except KeyboardInterrupt:
            logging.info('Quitting...')
            break
        except Exception as error:
            logging.error(error)
            raise


if __name__ == "__main__":
    dassort()
