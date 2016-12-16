import sys
from celery import chain

from tasks.galaxy import galaxydata
from tasks.galaxy import tasks
from tasks.galaxy import workflow_manage as wfmanage
from tasks.galaxy import util
from tasks.galaxy import nonwf_tasks
from tasks import config


def prep_workflow(parsefun):
    inputstore = {'params': {},
                  'galaxy_url': config.GALAXY_URL,
                  }
    inputstore['datasets'] = wfmanage.initialize_datasets()
    parsefun(inputstore)
    if inputstore['run'] == 'show':
        wfmanage.check_all_modules(inputstore)
        sys.exit()
    else:
        inputstore['wf'] = wfmanage.get_workflows()[inputstore['wf_num']]
        # Input checking. In UI we just demand inputs on the spot by reading
        # from the wf. Then we need to also specify the optional ones, but this
        # can be a start
        input_error = False
        # Library inputs are not checked because they are asked for
        for in_dset in inputstore['wf']['his_inputs']:
            if in_dset not in inputstore['wf']['required_inputs']:
                continue
            elif inputstore['datasets'][in_dset]['id'] is None:
                print('Dataset {} not specified. Exiting.'.format(in_dset))
                input_error = True
        for in_param in inputstore['wf']['param_inputs']:
            if in_param not in inputstore['wf']['required_inputs']:
                continue
            elif (in_param not in inputstore['params'] or
                    inputstore['params'][in_param] is None):
                print('Required parameter {} not specified. '
                      'Exiting.'.format(in_param))
                input_error = True
        for in_param in inputstore['wf']['other_inputs']:
            if in_param not in inputstore['wf']['required_inputs']:
                continue
            elif (in_param not in inputstore or
                    inputstore[in_param] is None):
                print('Required parameter {} not specified. '
                      'Exiting.'.format(in_param))
                input_error = True
        if input_error:
            sys.exit(1)
        gi = util.get_galaxy_instance(inputstore)
        libdsets = wfmanage.get_library_dsets(gi)
        inputstore['datasets'].update(libdsets)
        print('Using datasets from library:', libdsets)
    return inputstore, gi


def select_workflow():
    print('--------------------')
    workflows = wfmanage.get_workflows()
    for num, wf in enumerate(workflows):
        print(num, wf['name'])
    while True: 
        pick = input('Which workflow has been run? ')
        try:
            pick = int(pick)
        except ValueError:
            print('Please enter number separated with a comma)')
            continue
        else:
            break
    return {'wf': workflows[pick], 
            'module_uuids': wfmanage.get_modules_for_workflow(
                workflows[pick]['modules'])}


def get_modules_and_tasks(inputstore):
    runchain = []
    moduuids = {mname: muuid for muuid, mname in inputstore['module_uuids']}
    for modname in inputstore['wf']['modules']:
        if modname[0] == '@':
            runchain.append(nonwf_tasks.tasks[modname]['task'].s())
        else:
            runchain.append(tasks.run_workflow_module.s(moduuids[modname]))
    return runchain


def run_workflow(inputstore, gi, existing_spectra=False):
    """Runs a wf as specified in inputstore var"""
    inputstore['searchtype'] = inputstore['wf']['searchtype']
    inputstore['searchname'] = tasks.get_searchname(inputstore)
    if (inputstore['run'] == 1 and inputstore['rerun_his'] is None):
        # runs a single workflow composed of some modules
        inputstore['module_uuids'] = wfmanage.get_modules_for_workflow(
            inputstore['wf']['modules'])
        inputstore['g_modules'] = wfmanage.check_modules(
            gi, inputstore['module_uuids'])
        if inputstore['datasets']['spectra']['id'] is None:
            runchain = [tasks.tmp_create_history.s(inputstore),
                        tasks.tmp_put_files_in_collection.s(),
                        tasks.check_dsets_ok.s(), tasks.tmp_prepare_run.s()]
        else:
            runchain = [tasks.check_dsets_ok.s(inputstore), tasks.tmp_prepare_run.s()]
        if inputstore['params']['filesassets']:
            spectracollection = gi.histories.show_dataset_collection(
                inputstore['history'], inputstore['datasets']['spectra']['id'])
            sets = [x['object']['name'] for x in spectracollection['elements']]
            inputstore['params']['setnames'] = sets
            inputstore['params']['setpatterns'] = sets
        runchain.extend(get_modules_and_tasks(inputstore))
        runchain.extend(tasks.get_download_task_chain())
#    elif inputstore['run'] and len(inputstore['wf']) == 2:
#        # run two workflows with a history transition tool in between
#        inputstore['searchtype'] = inputstore['wf'][0]['searchtype']
#        firstwf_mods = get_modules_for_workflow(inputstore['wf'][0]['modules'])
#        second_wf_mods = get_modules_for_workflow(
#            inputstore['wf'][1]['modules'])
#        inputstore['module_uuids'] = firstwf_mods + second_wf_mods
#        inputstore['g_modules'] = tasks.check_modules(
#            gi, inputstore['module_uuids'])
#        runchain.extend([tasks.tmp_prepare_run.s()])
#        runchain.extend([tasks.run_workflow_module.s(mod_id[0])
#                         for mod_id in firstwf_mods])
#        runchain.extend(tasks.get_download_task_chain())
#        runchain.extend([tasks.reuse_history.s()])
#        runchain.extend([tasks.run_workflow_module.s(mod_id[0])
#                         for mod_id in second_wf_mods])
#        runchain.extend(tasks.get_download_task_chain())
    elif inputstore['run'] and inputstore['rerun_his']:
        # runs one workflow with a history to reuse from
        inputstore['module_uuids'] = wfmanage.get_modules_for_workflow(
            inputstore['wf']['modules'])
        inputstore['g_modules'] = wfmanage.check_modules(
            gi, inputstore['module_uuids'])
        runchain = [tasks.tmp_create_history.s(inputstore),
                    tasks.reuse_history.s(inputstore['rerun_his']),
                    tasks.tmp_put_files_in_collection.s(),
                    tasks.check_dsets_ok.s(),
                    ]
        runchain.extend(get_modules_and_tasks(inputstore))
        runchain.extend(tasks.get_download_task_chain())
    else:
        print('Not quite clear what you are trying to do here, '
              'would you like to --show workflows, run a vardb, or a normal'
              ' search?')
        sys.exit(1)
    res = chain(*runchain)
    res.delay()
