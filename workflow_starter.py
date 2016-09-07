import sys
from celery import chain

from tasks.galaxy import galaxydata
from tasks.galaxy import tasks
from tasks.galaxy import util
from tasks import config


def prep_workflow(parsefun):
    inputstore = {'params': {},
                  'galaxy_url': config.GALAXY_URL,
                  }
    inputstore['datasets'] = tasks.initialize_datasets()
    parsefun(inputstore)
    if inputstore['run'] == 'show':
        admin = {'galaxy_url': inputstore['galaxy_url'], 
                 'apikey': config.ADMIN_APIKEY}
        gi_admin = util.get_galaxy_instance(admin)
        for num, wf in enumerate(get_workflows()):
            modules = get_modules_for_workflow(wf['modules'])
            tasks.check_modules(gi_admin, modules)
            print('{}  -  {}'.format(num, wf['name']))
        sys.exit()
    else:
        gi = util.get_galaxy_instance(inputstore)
        libdsets = tasks.get_library_dsets(gi)
        inputstore['datasets'].update(libdsets)
        print('Using datasets from library:', libdsets)
        inputstore['wf'] = [get_workflows()[num]
                            for num in inputstore['wf_num']]
    return inputstore, gi


def select_workflow():
    print('--------------------')
    workflows = get_workflows()
    for num, wf in enumerate(workflows):
        print(num, wf['name'])
    while True: 
        picks = input('Which workflow (combination) has been run? '
                      'Separate combinations with commas: ')
        try:
            picks = [int(pick) for pick in picks.split(',')]
        except ValueError:
            print('Please enter number(s separated with a comma)')
            continue
        else:
            break
    modules = [get_modules_for_workflow(workflows[p]['modules']) for p in picks]
    return {'wf': [workflows[p] for p in picks], 
            'module_uuids': [y for x in modules for y in x]}


def get_workflows():
    return galaxydata.workflows


def get_modules_for_workflow(wf_mods):
    return [(galaxydata.wf_modules[m_name], m_name) for m_name in wf_mods]


def run_workflow(inputstore, gi, runchain):
    """Runs a wf as specified in inputstore var"""
    inputstore['searchtype'] = inputstore['wf'][0]['searchtype']
    inputstore['searchname'] = tasks.get_searchname(inputstore)
    inputstore['current_wf'] = 0
    if (inputstore['run'] and len(inputstore['wf']) == 1
            and inputstore['rerun_his'] is None):
        # runs a single workflow composed of some modules
        inputstore['module_uuids'] = get_modules_for_workflow(
            inputstore['wf'][0]['modules'])
        inputstore['g_modules'] = tasks.check_modules(
            gi, inputstore['module_uuids'])
        runchain.extend([tasks.tmp_prepare_run.s()])
        runchain.extend([tasks.run_workflow_module.s(mod_uuid[0])
                         for mod_uuid in inputstore['module_uuids']])
        runchain.extend(tasks.get_download_task_chain())
    elif inputstore['run'] and len(inputstore['wf']) == 2:
        # run two workflows with a history transition tool in between
        inputstore['searchtype'] = inputstore['wf'][0]['searchtype']
        firstwf_mods = get_modules_for_workflow(inputstore['wf'][0]['modules'])
        second_wf_mods = get_modules_for_workflow(
            inputstore['wf'][1]['modules'])
        inputstore['module_uuids'] = firstwf_mods + second_wf_mods
        inputstore['g_modules'] = tasks.check_modules(
            gi, inputstore['module_uuids'])
        runchain.extend([tasks.tmp_prepare_run.s()])
        runchain.extend([tasks.run_workflow_module.s(mod_id[0])
                         for mod_id in firstwf_mods])
        runchain.extend(tasks.get_download_task_chain())
        runchain.extend([tasks.reuse_history.s()])
        runchain.extend([tasks.run_workflow_module.s(mod_id[0])
                         for mod_id in second_wf_mods])
        runchain.extend(tasks.get_download_task_chain())
    elif inputstore['run'] and inputstore['rerun_his']:
        # runs one workflow with a history to reuse from
        inputstore['current_wf'] = -1  # set -1: reuse_history will increment 
        inputstore['history'] = inputstore['rerun_his']
        inputstore['module_uuids'] = get_modules_for_workflow(
            inputstore['wf'][0]['modules'])
        inputstore['g_modules'] = tasks.check_modules(
            gi, inputstore['module_uuids'])
        runchain.extend([tasks.reuse_history.s()])
        runchain.extend([tasks.run_workflow_module.s(mod_id[0])
                         for mod_id in inputstore['module_uuids']])
        runchain.extend(tasks.get_download_task_chain())
    else:
        print('Not quite clear what you are trying to do here, '
              'would you like to --show workflows, run a vardb, or a normal'
              ' search?')
        sys.exit(1)
    res = chain(*runchain)
    res.delay()
