import sys
import argparse

from tasks.galaxy import workflow_manage as wfmanage
from tasks import config
from tasks.galaxy import galaxydata

from workflow_starter import prep_workflow, run_workflow


TESTING_NO_CLEANUP = True


def main():
    inputstore, gi = prep_workflow(parse_commandline, parse_special_inputs)
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
    parser.add_argument('--test', dest='connectivity', action='store_const',
                        default=False, const=True)
    parser.add_argument('--reuse-history', dest='reuse_history')

    parser.add_argument('-w', dest='analysisnr', type=int)
    parser.add_argument('--sourcehists', dest='sourcehis', nargs='+')
    parser.add_argument('--sortspectra', dest='sort_specfiles', default=None,
                        action='store_const', const=True)
    parser.add_argument('--name', dest='searchname')
    parser.add_argument('--files-as-sets', dest='filesassets', default=False,
                        action='store_const', const=True)

    parser.add_argument('--setnames', dest='setnames', nargs='+')
    parser.add_argument('--setpatterns', dest='setpatterns', nargs='+')
    parser.add_argument('--isobtype', dest='multiplextype', default=None)
    parser.add_argument('--instrument', dest='instrument', default=None)
    parser.add_argument('--mods', dest='modifications', nargs='+')
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
    if args.show:
        inputstore['run'] = 'show'
    elif args.connectivity:
        inputstore['run'] = 'connectivity'
    else:
        inputstore['run'] = True
    for name in (wfstarter.get_flatfile_names_inputstore() +
                 wfstarter.get_collection_names_inputstore()):
        parsename = name.replace(' ', '_')
        if hasattr(args, parsename) and getattr(args, parsename) is not None:
            inputstore['datasets'][name]['id'] = getattr(args, parsename)
    for name in wfstarter.get_other_names_inputstore():
        parsename = name.replace(' ', '_')
        if hasattr(args, parsename) and getattr(args, parsename) is not None:
            inputstore['datasets'][name] = getattr(args, parsename)
    if args.filesassets and (args.setnames is not None or
                             args.setpatterns is not None):
        print('Conflicting input, --files-as-sets has been passed but '
              'also set definitions. Exiting.')
        sys.exit(1)
    for param in ['setnames', 'setpatterns', 'multiplextype', 'genefield',
                  'perco_ids', 'ppoolsize', 'fastadelim', 'filesassets',
                  'modifications', 'instrument', 'fr_matcher',
                  'strips', 'strippatterns', 'sort_specfiles']:
        if getattr(args, param) is not None:
            inputstore['params'][param] = getattr(args, param)
    if args.denominators is not None:
        inputstore['params']['denominators'] = ' '.join(args.denominators)
    inputstore['base_searchname'] = args.searchname
    inputstore['wf_num'] = args.analysisnr
    inputstore['rerun_his'] = args.reuse_history


def get_massshift(isobtype):
    return {'tmt10plex': '0.0013',
            }[isobtype]


def get_msgf_inputs(params):
    inputs = {'common_variable_modifications': [],
              'common_fixed_modifications': []}
    if params['multiplextype'] in ['tmt10plex', 'tmt6plex']:
        print('TMT10/6plex detected')
        protocol = '4'
        inputs['common_fixed_modifications'] = [
            '229.162932_*_fix_N-term_TMT6plex',
            '229.162932_K_fix_any_TMT6plex']
    elif params['multiplextype'][:5] == 'itraq' and not params['phospho']:
        print('iTRAQ detected')
        protocol = '2'
    elif params['multiplextype'][:5] == 'itraq' and params['phospho']:
        print('iTRAQ phospho detected')
        protocol = '3'
    elif params['phospho']:
        print('phospho detected')
        protocol = '1'
    else:
        print('No protocol detected, using automatic protocol for MSGF')
        protocol = '0'
    inputs['advanced|protocol'] = protocol
    if params['instrument'] == 'qe':
        inputs['inst'] = '3'
    elif params['instrument'] == 'velos':
        inputs['inst'] = '1'
    else:
        raise RuntimeError('Only pass qe or velos to --instrument')
    modifications = {'carba': 'C2H3N1O1_C_fix_any_Carbamidomethyl',
                     'ox': 'O1_M_opt_any_Oxidation',
                     }
    for inmod in params['modifications']:
        try:
            mod = modifications[inmod]
        except KeyError:
            raise RuntimeError('Only pass modifications "carba", "ox", or '
                               'update this code')
        modtype = mod.split('_')[2]
        if modtype == 'fix':
            inputs['common_fixed_modifications'].append(mod)
        elif modtype == 'opt':
            inputs['common_variable_modifications'].append(mod)
    return inputs


def assign_inputs_tools(inputstore):
    params = inputstore['params']
    if params['multiplextype'] is not None:
        params['IsobaricAnalyzer'] = {'param_extraction_reporter_mass_shift':
                                      get_massshift(params['multiplextype']),
                                      'param_type': params['multiplextype']}
    params['MS-GF+'] = get_msgf_inputs(params)
    params['Create nested list'] = {'batchsize': params['ppoolsize']}
    params['Get fraction numbers'] = {'code': params['fr_matcher']}
    params['FDR gene table'] = {}
    for toolid in ['Create gene table', 'Create protein table',
                   'Create symbol table', 'Create peptide table']:
        params[toolid] = {'isoquant|denompatterns': params['denominators']}


def parse_special_inputs(inputstore, gi):
    """Command line interface has some special inputs. Strips, filesassets,
    """
    params = inputstore['params']
    if params['filesassets']:
        spectracollection = gi.histories.show_dataset_collection(
            inputstore['history'], inputstore['datasets']['spectra']['id'])
        sets = [x['object']['name'] for x in spectracollection['elements']]
        params['setnames'] = sets
        params['setpatterns'] = sets
    if 'strips' in params:
        #'strips': [{'intercept': 3.5959, 'fr_width': 0.0174, 'name': '3-10'},
        #           {'intercept': 3.5478, 'fr_width': 0.0676}],
        #'strippatterns': ['IEF_37-49', 'IEF_3-10']}}
        params['strips'] = [galaxydata.strips[x] for x in params['strips']]
        params['strippatterns'] = ['"{}"'.format(x) for x
                                   in params['strippatterns']]
    if 'fr_matcher' in params:
        params['Get fraction numbers'] = {
            'code': ('s/{}/\\1/;s/\#SpecFile/'
                     'Fractions/'.format(params['fr_matcher']))}


if __name__ == '__main__':
    main()
