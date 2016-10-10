import sys
import os
import argparse

from tasks.galaxy import galaxydata
from tasks.galaxy import tasks
from tasks.galaxy import util
from tasks import config

from workflow_starter import prep_workflow, run_workflow


TESTING_NO_CLEANUP = True

def main():
    inputstore, gi = prep_workflow(parse_commandline)
    if inputstore['user'] != config.ADMIN_USER:
        inputstore = tasks.transfer_workflow_modules(inputstore)
    runchain = [
                tasks.tmp_create_history.s(inputstore),
                tasks.tmp_put_files_in_collection.s(),
                tasks.check_dsets_ok.s()]
    run_workflow(inputstore, gi, runchain) 


def parse_commandline(inputstore):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-u', dest='user')
    parser.add_argument('--outshare', dest='outshare')
    parser.add_argument('--show', dest='show', action='store_const',
                        default=False, const=True)
    parser.add_argument('--reuse-history', dest='reuse_history')
    parser.add_argument('-w', dest='analysisnr', type=int, nargs=True)
    parser.add_argument('--sourcehists', dest='sourcehistories', nargs='+')
    #parser.add_argument('-d', dest='target_db')
    #parser.add_argument('-m', dest='modifications')
    parser.add_argument('--name', dest='searchname')
    #parser.add_argument('--mart', dest='biomart_map')
    parser.add_argument('--setnames', dest='setnames', nargs='+')
    parser.add_argument('--setpatterns', dest='setpatterns', nargs='+')
    parser.add_argument('--isobtype', dest='isobtype', default=None)
    parser.add_argument('--denominators', dest='denominators', nargs='+')
    parser.add_argument('--ppool-ids', dest='perco_ids', nargs='+')
    parser.add_argument('--ppool-size', dest='ppoolsize', default=8)
    parser.add_argument('--fastadelim', dest='fastadelim', type=str)
    parser.add_argument('--genefield', dest='genefield', type=int)
    parser.add_argument('--knownproteins', dest='knownpep_db')
    args = parser.parse_args(sys.argv[1:])
    inputstore['user'] = args.user
    inputstore['apikey'] = config.USERS[args.user][1]
    inputstore['outshare'] = args.outshare
    inputstore['sourcehis'] = args.sourcehistories
    if args.show:
        inputstore['run'] = 'show'
    else:
        inputstore['run'] = True
    for name in inputstore['datasets']:
        parsename = name.replace(' ', '_')
        if hasattr(args, parsename) and getattr(args, parsename) is not None:
            inputstore['datasets'][name]['id'] = getattr(args, parsename)
    for param in ['setnames', 'setpatterns', 'isobtype', 'genefield',
                  'perco_ids', 'ppoolsize', 'fastadelim']:
        if getattr(args, param) is not None:
            inputstore['params'][param] = getattr(args, param)
    if args.denominators is not None:
        inputstore['params']['denominators'] = ' '.join(args.denominators)
    inputstore['base_searchname'] = args.searchname
    inputstore['wf_num'] = args.analysisnr
    inputstore['rerun_his'] = args.reuse_history


if __name__ == '__main__':
    main()
