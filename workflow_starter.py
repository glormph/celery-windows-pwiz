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
    gi = util.get_galaxy_instance(inputstore)
    parsefun(inputstore, gi)
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


def check_workflow_mod_connectivity(workflows, inputstore, dry_run=False):
    """This method has 3 uses:
        - Seeing if all datasets have been supplied for a run
        - Seeing if all datasets are supplied in a restarted run
        - Dry run a workflow to test if workflow modules are connected
    """
    gi = util.gget_galaxy_instance(inputstore)
    mods_inputs = {}
    mods_outputs = {}
    galaxy_modules = wfmanage.check_all_modules(inputstore)
    connect_ok = True
    for wf in workflows:
        print('Checking workflow connectivity for {}'.format(wf['name']))
        if dry_run:
            allinputs = (wf['lib_inputs'] + wf['required_dsets'] +
                         wf['required_params'])
        else:
            allinputs = [x for x in inputstore['datasets']
                         if x['id'] is not None]
            allinputs += [x for x in inputstore['params']]
        allinputs += wf['not_used_tool_inputs']
        for mod in wf['modules']:
            if mod[0] == '@' and not mod in mods_inputs:
                mods_inputs[mod] = nonwf_tasks.tasks[mod]['inputs']
                mods_inputs[mod].extend(nonwf_tasks.tasks[mod]['params'])
                mods_outputs[mod] = nonwf_tasks.tasks[mod]['outputs']
            elif not mod in mods_inputs:
                gmod = gi.workflows.show_workflow(galaxy_modules[mod]['id'])
                mods_inputs[mod] = [x[0] for x in
                                    wfmanage.get_workflow_inputs(gmod)]
                for param in wfmanage.get_workflow_params(gmod):
                    mods_inputs[mod].append(param['name'])
                mod_uuid = galaxydata.wf_modules[mod]
                wf_json = gi.workflows.export_workflow_json(mod_uuid)
                mods_outputs[mod] = wfmanage.get_workflow_outputs(wf_json)
            if not wfmanage.check_workflow_inputs_ok(mod, mods_inputs[mod],
                                                     allinputs):
                connect_ok = False
            allinputs.extend(mods_outputs[mod])
    if not connect_ok:
        print('Problems in workflow connectivity')
    else:
        print('Workflows ok')
    return connect_ok


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
            runchain = [tasks.check_dsets_ok.s(inputstore),
                        tasks.tmp_prepare_run.s()]
        runchain.extend(get_modules_and_tasks(inputstore))
        runchain.extend(tasks.get_download_task_chain())
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
