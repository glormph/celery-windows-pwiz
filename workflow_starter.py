import sys
import json
from celery import chain
from datetime import datetime

from tasks.galaxy import galaxydata
from tasks.galaxy import json_workflows
from tasks.galaxy import tasks
from tasks.galaxy import workflow_manage as wfmanage
from tasks.galaxy import util
from tasks.galaxy import nonwf_tasks


def prep_inputs(inputstore, parsespecial):
    # FIXME this is a new method, untested
    """Input checking. In UI we just demand inputs on the spot by reading
    from the wf. Then we need to also specify the optional ones, but this
    can be a start"""
    gi = util.get_galaxy_instance(inputstore)
    parsespecial(inputstore, gi)
    input_error = False
    # Library inputs are not checked because they are asked for
    for in_dset in inputstore['datasets']:
        if in_dset not in inputstore['wf']['required_dsets']:
            continue
        elif in_dset in wfmanage.get_other_names_inputstore():
            checkval = inputstore['datasets'][in_dset]
        else:
            checkval = inputstore['datasets'][in_dset]['id']
        if checkval is None:
            print('Dataset or parameter {} not specified. '
                  'Exiting.'.format(in_dset))
            input_error = True
    for in_param in inputstore['wf']['required_params']:
        if (in_param not in inputstore['params'] or
                inputstore['params'][in_param] is None):
            print('Required parameter {} not specified. '
                  'Exiting.'.format(in_param))
            input_error = True
    if input_error:
        return False
    print('All data locally defined as required has been passed')
    return inputstore


def prep_libdsets(inputstore):
    gi = util.get_galaxy_instance(inputstore)
    libdsets = wfmanage.get_library_dsets(gi, inputstore['wf']['lib_inputs'])
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
    gi = util.get_galaxy_instance(inputstore)
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
            allinputs = [x for x, val in inputstore['datasets'].items()
                         if type(val) == dict and val['id'] is not None]
            allinputs += [x for x, val in inputstore['datasets'].items()
                          if type(val) != dict and val is not None]
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


def get_searchname(inputstore):
    return '{}_{}'.format(inputstore['base_searchname'],
                          inputstore['searchtype'])


def add_repeats_to_workflow_json(inputstore, wf_json):
    """Takes as input wf_json the thing the output from
    gi.workflows.export_workflow_json"""
    print('Updating set names and connecting loose step (percolator-in)')
    params = inputstore['params']
    strip_list = json.dumps([{'__index__': ix, 'intercept': strip['intercept'],
                              'fr_width': strip['fr_width'],
                              'pattern': strippat} for ix, (strip, strippat) in
                             enumerate(zip(params['strips'],
                                           params['strippatterns']))])
    ppool_list = json.dumps([{'__index__': ix, 'pool_identifier': name}
                             for ix, name in enumerate(params['perco_ids'])])
    set_list = json.dumps([{'__index__': ix, 'pool_identifier': name}
                           for ix, name in enumerate(params['setpatterns'])])
    lookup_list = json.dumps([{'__index__': ix, 'set_identifier': setid,
                               'set_name': setname} for ix, (setid, setname) in
                              enumerate(zip(params['setpatterns'],
                                            params['setnames']))])
    percin_input_stepids = set()
    # Add setnames to repeats, pi strips to delta-pi-calc
    for step in wf_json['steps'].values():
        state_dic = json.loads(step['tool_state'])
        if step['tool_id'] is None:
            continue
        elif 'batched_set' in step['tool_id']:
            if 'RuntimeValue' in state_dic['batchsize']:
                # also find decoy perco-in batch ID
                percin_input_stepids.add(step['id'])
                state_dic['poolids'] = ppool_list
            else:
                state_dic['poolids'] = set_list
            step['tool_state'] = json.dumps(state_dic)
        elif 'msslookup_spectra' in step['tool_id']:
            state_dic['pools'] = lookup_list
            step['tool_state'] = json.dumps(state_dic)
        elif 'calc_delta_pi' in step['tool_id']:
            state_dic['strips'] = strip_list
            step['tool_state'] = json.dumps(state_dic)
    return connect_percolator_in_steps(wf_json, percin_input_stepids)


def connect_percolator_in_steps(wf_json, percin_input_stepids):
    # connect percolator in step
    for step in wf_json['steps'].values():
        if (step['tool_id'] is not None and
                'percolator_input_converters' in step['tool_id']):
            step_input = step['input_connections']
            percin_input_stepids.remove(step_input['mzids|target']['id'])
            step_input['mzids|decoy'] = {
                'output_name': 'batched_fractions_mzid',
                'id': percin_input_stepids.pop()}
    return wf_json


def get_versioned_module(modname, version):
    # FIXME db call to get a version of the modname
    # FIXME create gh repo with versions
    with open('json_workflows/{}_v{}.json'.format(modname, version)) as fp:
        return json.load(fp)


def fill_runtime_params(step, params):
    """Should return step tool_id, name, composed_name"""
    tool_param_inputs = json.loads(step['tool_state'])
    # Use annotation to define step name in case that is given, to be able
    # to get steps like sed which have a very general name and may occur more
    # than once
    annot = step['annotation']
    stepname = annot[:annot.index('---')] if annot else step['name']
    dset_input_names = [x['name'] for x in step['inputs'] if not
                        x['description'].startswith('runtime parameter')]
    for input_name, input_val in tool_param_inputs.items():
        if input_name in dset_input_names:
            continue
        try:
            input_val = json.loads(input_val)
        except (TypeError, ValueError):
            # no json obj, no runtime values
            continue
        if type(input_val) == dict:
            # Only runtime vals or conditionals are dict, simple values
            # are just strings
            if dict in [type(x) for x in input_val.values()]:
                # complex input dict from e.g. conditional which contains
                # a dict possibly containing runtime value
                # TODO make this recursive maybe for nested conditionals
                for subname, subval in input_val.items():
                    composed_name = '{}|{}'.format(input_name, subname)
                    if is_runtime_param(subval, composed_name, step):
                        input_val[subname] = params[stepname][composed_name]
                tool_param_inputs[input_name] = json.dumps(input_val)
            else:
                if is_runtime_param(input_val, input_name, step):
                    try:
                        tool_param_inputs[input_name] = json.dumps(
                            params[stepname][input_name])
                    except:
                        print('WARNING, RuntimeValue for tool {}, param {} '
                              'expected, but nothing passed (possibly is an '
                              'unneeded dataset though).'.format(stepname,
                                                                 input_name))
    step['tool_state'] = json.dumps(tool_param_inputs)


def get_spectraquant_wf(inputstore):
    if 'IsobaricAnalyzer' in inputstore['params']:
        wf_fn = 'json_workflows/spectra_quant_isobaric.json'
    else:
        wf_fn = 'json_workflows/spectra_quant_labelfree.json'
    with open(wf_fn) as fp:
        return json.load(fp)


def get_step_tool_states(wf_json):
    return {step['id']: json.loads(step['tool_state'])
            for step in wf_json['steps'].values()}


def get_input_dset_step_id_for_name(tool_states, name):
    return [step_id for step_id, ts in tool_states.items()
            if 'name' in ts and ts['name'] == name][0]


def connect_specquant_workflow(spec_wf_json, search_wf_json):
    print('Connecting spectra quant workflow to search workflow')
    step_tool_states = get_step_tool_states(search_wf_json)
    # first remove quant lookup input
    qlookup_step_id = get_input_dset_step_id_for_name(step_tool_states,
                                                      'quant lookup')
    remove_step_from_wf(qlookup_step_id, search_wf_json)
    # to make space for spec quant, change ID on all steps, and all connections
    first_tool_stepnr = min([x['id'] for x in search_wf_json['steps'].values()
                             if x['tool_id'] is not None])
    if first_tool_stepnr > qlookup_step_id:
        first_tool_stepnr -= 1
    amount_spec_steps = len([step for step in spec_wf_json['steps'].values()
                             if step['tool_id'] is not None])
    newsteps = {}
    for step in search_wf_json['steps'].values():
        if step['id'] >= first_tool_stepnr:
            step['id'] = step['id'] + amount_spec_steps
            for connection in step['input_connections'].values():
                if connection['id'] >= first_tool_stepnr:
                    connection['id'] = connection['id'] + amount_spec_steps
        newsteps[str(step['id'])] = step
    search_wf_json['steps'] = newsteps
    # Subtract 1 because we have removed an input step (quant lookup)
    spec_step_id = get_input_dset_step_id_for_name(step_tool_states, 'spectra')
    if spec_step_id > qlookup_step_id:
        spec_step_id -= 1
    # Add spectra/quant steps, connect to spectra collection input
    for step in spec_wf_json['steps'].values():
        if step['tool_id'] is None:
            continue
        step['id'] = step['id'] - 1 + first_tool_stepnr
        for connection in step['input_connections'].values():
            connection['id'] = connection['id'] - 1 + first_tool_stepnr
        if 'spectra' in step['input_connections']:
            step['input_connections']['spectra']['id'] = spec_step_id
        elif 'ms1_in' in step['input_connections']:
            step['input_connections']['ms1_in']['id'] = spec_step_id
        elif 'param_in' in step['input_connections']:
            step['input_connections']['param_in']['id'] = spec_step_id
        search_wf_json['steps'][str(step['id'])] = step
        if step['name'] == 'Create lookup table with quant data':
            lookupstep = step
    # Connect to PSM table
    for step in search_wf_json['steps'].values():
        if step['name'] == 'Process PSM table':
            step['input_connections']['lookup']['id'] = lookupstep['id']


def remove_ensembl_steps(wf_json):
    print('Removing ENSEMBL steps and inputs from workflow')
    # remove biomart from PSM table input connections
    for step in wf_json['steps'].values():
        if step['name'] == 'Process PSM table':
            del(step['input_connections']['mapfn'])
            step['inputs'] = [x for x in step['inputs']
                              if x['name'] != 'mapfn']
    # remove biomart input, update all IDs that come from there
    step_tool_states = get_step_tool_states(wf_json)
    mart_step_id = get_input_dset_step_id_for_name(step_tool_states,
                                                   'biomart map')
    remove_step_from_wf(mart_step_id, wf_json)
    # remove all boxes which say symbol_table in annotation
    symbol_table = True
    while symbol_table:
        symbol_table = False
        for step in wf_json['steps'].values():
            annot = step['annotation']
            stepname = annot[:annot.index('---')] if annot else step['name']
            if 'symbol table' in stepname:
                remove_step_from_wf(step['id'], wf_json)
                symbol_table = True
                break


def remove_step_from_wf(removestep_id, wf_json):
    del(wf_json['steps'][str(removestep_id)])
    newsteps = {}
    for step in wf_json['steps'].values():
        if step['id'] > removestep_id:
            step['id'] -= 1
        for connection in step['input_connections'].values():
            if connection['id'] > removestep_id:
                connection['id'] -= 1
        newsteps[str(step['id'])] = step
    wf_json['steps'] = newsteps


def fill_in_iso(step, isokey, subkey, value):
    ts = json.loads(step['tool_state'])
    iso_ts = json.loads(ts[isokey])
    iso_ts[subkey] = value
    ts[isokey] = json.dumps(iso_ts)
    step['tool_state'] = json.dumps(ts)


def disable_isobaric_params(wf_json, proteincentric=False):
    print('Removing isobaric steps and inputs from workflow')
    for step in wf_json['steps'].values():
        if step['name'] == 'Process PSM table':
            fill_in_iso(step, 'isobaric', 'yesno', 'false')
            fill_in_iso(step, 'isobaric', 'denompatterns', '')
        elif step['name'] == 'Create peptide table':
            fill_in_iso(step, 'isoquant', 'yesno', 'false')
            fill_in_iso(step, 'isoquant', 'denompatterns', '')
        elif step['name'] == 'Merge peptide or protein tables':
            ts = json.loads(step['tool_state'])
            ts['isobqcolpattern'] = json.dumps('')
            ts['nopsmcolpattern'] = json.dumps('')
            step['tool_state'] = json.dumps(ts)
        elif proteincentric and step['name'] == 'Create protein table':
            fill_in_iso(step, 'isoquant', 'yesno', 'false')
            fill_in_iso(step, 'isoquant', 'denompatterns', '')


def remove_isobaric_from_peptide_centric(wf_json):
    disable_isobaric_params(wf_json)
    step_toolstates = get_step_tool_states(wf_json)
    psmstep = get_input_dset_step_id_for_name(step_toolstates,
                                              'PSM table target normalsearch')
    for step in wf_json['steps'].values():
        if (step['name'] == 'Split tabular data' and
                step['input_connections']['input']['id'] == psmstep):
            remove_step_from_wf(step['id'], wf_json)
            break
    remove_step_from_wf(psmstep, wf_json)


def remove_isobaric_from_protein_centric(wf_json):
    # Remove normalize generating step
    for step in wf_json['steps'].values():
        annot = step['annotation']
        stepname = annot[annot.index('---') + 3:] if annot else step['name']
        if stepname.strip() == 'Normalization-ratio generating step':
            remove_step_from_wf(step['id'], wf_json)
            break
    disable_isobaric_params(wf_json, proteincentric=True)


def is_runtime_param(val, name, step):
    try:
        isruntime = val['__class__'] == 'RuntimeValue'
    except (KeyError, TypeError):
        return False
    else:
        if isruntime and name not in step['input_connections']:
            return True
        return False


def new_run_workflow(inputstore, gi):
    """Passed a workflow in inputstore, this function will create a runchain
    of tasks for celery. Will either use the existing celery task or fetch
    workflow JSON, mend and fill it and create a celery task for it.
    It then queues tasks to celery"""
    # FIXME if quant lookup exists, do not use specquant wf
    #inputstore = wfmanage.new_transfer_workflow_modules(inputstore)
    # wf passed is {'version': x, 'mods': [(name, uuid), (name, uuid)]
    inputstore['searchtype'] = inputstore['wf']['searchtype']
    inputstore['searchname'] = get_searchname(inputstore)
    runchain = [tasks.tmp_create_history.s(inputstore),
                tasks.check_dsets_ok.s()]
    inputstore['wf']['uploaded'] = {}
    for module in inputstore['wf']['modules']:
        if module[0][0] == '@':
            runchain.append(nonwf_tasks.tasks[module[0]]['task'].s())
        else:
            modname, version, modtype = module[0], module[1], module[2]
            raw_json = get_versioned_module(modname, version)
            if (modtype == 'search' and
                    inputstore['datasets']['quant lookup']['id'] is None):
                specquant_wfjson = get_spectraquant_wf(inputstore)
                connect_specquant_workflow(specquant_wfjson, raw_json)
            wf_json = add_repeats_to_workflow_json(inputstore, raw_json)
            if modtype == 'search' and inputstore['wf']['dbtype'] != 'ensembl':
                remove_ensembl_steps(wf_json)
            if inputstore['wf']['quanttype'] == 'labelfree':
                if modtype == 'peptides noncentric':
                    remove_isobaric_from_peptide_centric(wf_json)
                if modtype == 'proteingenes':
                    remove_isobaric_from_protein_centric(wf_json)
            print('Filling in runtime values...')
            for step in wf_json['steps'].values():
                fill_runtime_params(step, inputstore['params'])
            uploaded = gi.workflows.import_workflow_json(wf_json)
            inputstore['wf']['uploaded'][modname] = uploaded['id']
            runchain.append(tasks.run_workflow_module.s(uploaded['id']))
    runchain.extend(tasks.get_download_task_chain())
    res = chain(*runchain)
    res.delay()


def run_workflow(inputstore, gi, existing_spectra=False):
    """Runs a wf as specified in inputstore var"""
    # ###### NEW PLAN
    # get wfjsons from VC place so you can pick a version
    # mend repeats and connections
    # fill in runtime params
    # upload
    # run it
    #
    # 6RF has some extras, need to create a DB in advance
    # cannot easily be run as ONE wf, so use the old strategy of connecting
    # but still do wf uploads first
    #
    ########
    inputstore['searchtype'] = inputstore['wf']['searchtype']
    inputstore['searchname'] = get_searchname(inputstore)
    if (inputstore['run'] == 1 and inputstore['rerun_his'] is None):
        # runs a single workflow composed of some modules
        inputstore['module_uuids'] = wfmanage.get_modules_for_workflow(
            inputstore['wf']['modules'])
        inputstore['g_modules'] = wfmanage.check_modules(
            gi, inputstore['module_uuids'])
        if inputstore['datasets']['spectra']['id'] is None:
            runchain = [tasks.tmp_create_history.s(inputstore),
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
