import sys
from datetime import datetime
import bioblend

from tasks import config
from tasks.galaxy import galaxydata
from tasks.galaxy import util
import fixtures
from cli import parse_commandline, parse_special_inputs, show_pattern_matchers
import workflow_starter as wfstarter


TESTING_NO_CLEANUP = True


def main():
    # old: g_modules = {uuid: show_workflow}
    # prep does not need show, that can be own function
    # include in show also date/version/commit message for wfs.
    # connectivity check, hmmmm
    #
    inputstore = {'params': {},
                  'galaxy_url': config.GALAXY_URL,
                  }
    inputstore['datasets'] = wfstarter.initialize_datasets()
    parse_commandline(inputstore)
    if inputstore['run'] == 'show':
        # display local modules
        show()
        return
    inputstore['apikey'] = config.USERS[inputstore['user']][1]
    gi = util.get_galaxy_instance(inputstore)
    if inputstore['run'] == 'newuser':
        util.create_user(inputstore)
    elif inputstore['run'] == 'test':
        test(inputstore, gi)
    elif inputstore['run'] == 'connectivity':
        pass
        # do connectivity check on local modules
        #wfmanage.check_workflow_mod_connectivity(galaxydata.workflows,
        #inputstore, gi, dry_run=True)
    else:
        parse_special_inputs(inputstore, gi)
        assign_inputs_tools(inputstore)
        show_pattern_matchers(inputstore, gi)
        inputstore['wf'] = wfstarter.get_workflows()[inputstore['wf_num']]
        inputstore = wfstarter.check_required_inputs(inputstore, gi)
        if not inputstore:
            print('Errors encountered. Exiting.')
            sys.exit(1)
        inputstore = wfstarter.get_libdsets(inputstore, gi)
        #if not wfstarter.check_workflow_mod_connectivity([inputstore['wf']],
        #                                                 inputstore, gi):
        #    print('Workflow connectivity is not validated, ERROR')
        #    sys.exit(1)
        wfstarter.run_workflow(inputstore, gi)


def show():
    print('-------------------- WORKFLOWS -------------')
    for ix, wf in enumerate(galaxydata.workflows):
        print(ix, wf['name'])
    while True:
        pick = input('Enter selection for more info: ')
        if pick == '':
            break
        try:
            pick = int(pick)
        except ValueError:
            print('Please enter a number corresponding to a dataset or '
                  'enter')
            continue
        break
    if pick != '':
        print(galaxydata.workflows[pick])


def test(inputstore, gi):
    # FIXME think about what tests are needed
    # - new workflow in galaxy test
    # - check alll WFs in json with params test in case this code changes
    """Download a base WF from galaxy, fill it in and upload
    CLI inputs are
    --galaxy-wf ID-OF-WF-TO-TEST-ON-GALAXY
    --wftype [proteingenes, proteingenesymbols"""
    inputstore['params'] = fixtures.UNIPROT_PARAMS
    inputstore['wf'] = {'quanttype': 'isobaric', 'uploaded': {}}
    failcount = 0
    for wftype in inputstore['wftype']:
        base_wf = gi.workflows.export_workflow_json(inputstore['galaxy_wf'])
        print('--------------TESTING {} ---------'.format(wftype))
        inputstore['searchname'] = 'TEST {} - {}'.format(base_wf['name'],
                                                         wftype)
        timest = datetime.strftime(datetime.now(), '%Y%m%d_%H.%M')
        try:
            inputstore, up_id = wfstarter.finalize_galaxy_workflow(base_wf,
                                                                   wftype,
                                                                   inputstore,
                                                                   timest, gi)
        except bioblend.ConnectionError:
            failcount += 1
            print('-------------- FAILURE ---------'.format(wftype))
            print('Unsuccesful upload of finalized workflow {} using type '
                  '{}'.format(base_wf['name'], wftype))
        else:
            print('-------------- SUCCESS ---------'.format(wftype))
            print('OK - {} -- {} workflow ID {}'.format(base_wf['name'],
                                                        wftype, up_id))
    print('=============================')
    if failcount:
        print('{} failures, {} success'.format(
            failcount, len(inputstore['wftype']) - failcount))
    else:
        print('ALL TESTS PASS')


def get_massshift(isobtype):
    return {
        'tmt10plex': '0.0013',
        'tmt6plex': '0.0013',
        'itraq8plex': '0.0011',
            }[isobtype]


def get_msgf_inputs(params):
    inputs = {'common_variable_modifications': [],
              'common_fixed_modifications': []}
    if params['multiplextype'] is None and not params['phospho']:
        print('No multiplex or phospho, using automatic protocol for MSGF')
        protocol = '0'
    elif params['multiplextype'] is None and params['phospho']:
        print('phospho detected')
        protocol = '1'
    elif params['multiplextype'] in ['tmt10plex', 'tmt6plex']:
        print('TMT10/6plex detected')
        protocol = '4'
        inputs['common_fixed_modifications'] = [
            '229.162932_*_fix_N-term_TMT6plex',
            '229.162932_K_fix_any_TMT6plex']
    elif params['multiplextype'][:5] == 'itraq' and not params['phospho']:
        print('iTRAQ detected')
        protocol = '2'
        inputs['common_fixed_modifications'] = [
            '304.205360_K_fix_any_iTRAQ8plex',
            '304.205360_*_fix_N-term_iTRAQ8plex',
        ]
    elif params['multiplextype'][:5] == 'itraq' and params['phospho']:
        print('iTRAQ phospho detected')
        protocol = '3'
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
    if params['custommods']:
        mods = []
        for mod in params['custommods']:
            mod = mod.split('_')
            mods.append({'mass': mod[0], 'aa': list(mod[1]), 'fo': mod[2],
                         'pos': mod[3], 'name': mod[4]})
        params['custommods'] = mods
        print(params['custommods'])
    for inmod in params['modifications']:
        try:
            mod = modifications[inmod]
        except KeyError:
            raise RuntimeError('Only pass modifications {} or update this code'
                               ''.format(', '.join(modifications.keys())))
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
        params['msstitch QC'] = {'isobaric': 'true'}
    else:
        params['msstitch QC'] = {'isobaric': 'false'}
    params['msstitch QC'].update({
        'setnames': ' '.join(params['setnames']),
        'platepatterns': ' '.join(params['strippatterns'])})
    params['MS-GF+'] = get_msgf_inputs(params)
    params['Create nested list'] = {'batchsize': params['ppoolsize']}
    params['FDR gene table'] = {}  # FIXME why


if __name__ == '__main__':
    main()
