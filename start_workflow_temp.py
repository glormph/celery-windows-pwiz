import sys
import argparse

from tasks.galaxy import workflow_manage as wfmanage
from tasks import config

from workflow_starter import prep_workflow, run_workflow


TESTING_NO_CLEANUP = True


def main():
    inputstore, gi = prep_workflow(parse_commandline)
    if inputstore['user'] != config.ADMIN_USER:
        inputstore = wfmanage.transfer_workflow_modules(inputstore)
    run_workflow(inputstore, gi)


def parse_commandline(inputstore):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-u', dest='user')
    parser.add_argument('--outshare', dest='outshare')
    parser.add_argument('--show', dest='show', action='store_const',
                        default=False, const=True)
    parser.add_argument('--reuse-history', dest='reuse_history')
    parser.add_argument('-w', dest='analysisnr', type=int)
    parser.add_argument('--sourcehists', dest='sourcehistories', nargs='+')
    parser.add_argument('--name', dest='searchname')
    parser.add_argument('--files-as-sets', dest='filesassets', default=False,
                        action='store_const', const=True)
    parser.add_argument('--setnames', dest='setnames', nargs='+')
    parser.add_argument('--setpatterns', dest='setpatterns', nargs='+')
    parser.add_argument('--isobtype', dest='multiplextype', default=None)
    parser.add_argument('--denominators', dest='denominators', nargs='+')
    parser.add_argument('--strips', dest='strips', nargs='+', help='Specify '
                        'which strips have been used in split DB experiments '
                        'where DBs are pI predicted')
    parser.add_argument('--strippatterns', dest='strippatterns', nargs='+',
                        help='Need to have same order as strips '
                        'in --strips')
    parser.add_argument('--frpattern', dest='fr_matcher',
                        help='Use this regex pattern to match fraction number '
                        'in filenames, for multiDB. E.g: .*fr([0-9][0-9]).*'
                        )
    parser.add_argument('--pipeptides', dest='pipeptides_db')
    parser.add_argument('--pipeptides-known', dest='pipeptides_known_db')
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
    #    if name in tasks.get_multidset_names_inputstore():
    #        continue
        parsename = name.replace(' ', '_')
        if hasattr(args, parsename) and getattr(args, parsename) is not None:
            inputstore['datasets'][name]['id'] = getattr(args, parsename)
    if args.filesassets and (args.setnames is not None or
                             args.setpatterns is not None):
        print('Conflicting input, --files-as-sets has been passed but '
              'also set definitions. Exiting.')
        sys.exit(1)
    #for name in tasks.get_multidset_names_inputstore():
    #    parsename = name.replace(' ', '_')
    #    if hasattr(args, parsename) and getattr(args, parsename) is not None:
    #        inputstore['datasets'][name].append(
    #            {'src': 'hdca', 'id': getattr(args, parsename)})
    for param in ['setnames', 'setpatterns', 'multiplextype', 'genefield',
                  'perco_ids', 'ppoolsize', 'fastadelim', 'filesassets',
                  'strips', 'pipatterns']:
        if getattr(args, param) is not None:
            inputstore['params'][param] = getattr(args, param)
    if args.denominators is not None:
        inputstore['params']['denominators'] = ' '.join(args.denominators)
    inputstore['base_searchname'] = args.searchname
    inputstore['wf_num'] = args.analysisnr
    inputstore['rerun_his'] = args.reuse_history


if __name__ == '__main__':
    main()
