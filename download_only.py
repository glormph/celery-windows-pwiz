import sys
import os
import argparse
from celery import chain

from tasks.galaxy import galaxydata
from tasks.galaxy import tasks
from tasks.galaxy import util
from tasks import config
from workflow_starter import select_workflow

TESTING_NO_CLEANUP = True


def main():
    inputstore = {'params': {},
                  'galaxy_url': config.GALAXY_URL,
                  }
    inputs = {name: {'src': 'hda', 'id': None} for name in
              get_flatfile_names_inputstore()}
    inputs.update({name: {'src': 'hdca', 'id': None} for name in
                   get_collection_names_inputstore()})
    inputstore['datasets'] = inputs
    inputstore.update(select_workflow())
    parse_commandline(inputstore)
    gi = util.get_galaxy_instance(inputstore)
    # FIXME in kantele system keep orig name in DB to avoid tampering by user
    inputstore['searchname'] = gi.histories.show_history(
        inputstore['history'])['name']
    inputstore['rerun_his'] = 'NA'
    inputstore['current_wf'] = 0
    runchain = tasks.get_download_task_chain(inputstore)
    res = chain(*runchain)
    res.delay()


def parse_commandline(inputstore):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-u', dest='user')
    parser.add_argument('--history', dest='history')
    parser.add_argument('--outshare', dest='outshare')
    args = parser.parse_args(sys.argv[1:])
    inputstore['user'] = args.user
    inputstore['history'] = args.history
    inputstore['apikey'] = config.USERS[args.user][1]
    inputstore['outshare'] = args.outshare


def get_output_dsets():
    return galaxydata.download_data_names


def get_flatfile_names_inputstore():
    return galaxydata.flatfile_names


def get_collection_names_inputstore():
    return galaxydata.collection_names

 
if __name__ == '__main__':
    main()
