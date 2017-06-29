import json
from celery import chain
from datetime import datetime

from tasks.galaxy import galaxydata
from tasks.galaxy import tasks
from tasks.galaxy import nonwf_tasks


def get_workflows():
    return galaxydata.workflows


def initialize_datasets():
    """Fills inputstore with empty dict of datasets which are to be
    made by Galaxy"""
    inputs = {name: {'src': 'hda', 'id': None} for name in
              get_flatfile_names_inputstore()}
    inputs.update({name: {'src': 'hdca', 'id': None} for name in
                   get_collection_names_inputstore()})
    return inputs


def get_flatfile_names_inputstore():
    return galaxydata.flatfile_names


def get_collection_names_inputstore():
    return galaxydata.collection_names


def check_required_inputs(inputstore, gi):
    # FIXME this is a new method, untested
    """Input checking. In UI we just demand inputs on the spot by reading
    from the wf spec in the galaxydata module.
    Then we need to also specify the optional ones, but this can be a start"""
    input_error = False
    # Library inputs are not checked because they are asked for
    for in_dset in inputstore['datasets']:
        if in_dset not in inputstore['wf']['required_dsets']:
            continue
        else:
            checkval = inputstore['datasets'][in_dset]['id']
        if checkval is None:
            print('Dataset or parameter {} not specified.'.format(in_dset))
            input_error = True
    for in_param in inputstore['wf']['required_params']:
        if (in_param not in inputstore['params'] or
                inputstore['params'][in_param] is None):
            print('Required parameter {} not specified.'.format(in_param))
            input_error = True
    if input_error:
        return False
    print('All data locally defined as required has been passed')
    return inputstore


def get_libdsets(inputstore, gi):
    # FIXME this is a CLI thing
    all_picked_libdsets = []
    for required_libdset in inputstore['wf']['lib_inputs']:
        if inputstore['datasets'][required_libdset]['id'] is None:
            pick_libds = get_library_dset(gi, required_libdset)
            if pick_libds:
                inputstore['datasets'][required_libdset] = pick_libds
                all_picked_libdsets.append(pick_libds)
    print('Using datasets from library:', all_picked_libdsets)
    return inputstore


def get_library_dset(gi, lib_dset_name):
    # FIXME this is a CLI thing
    dset_names_libname = {'target db': 'databases',
                          'decoy db': 'databases',
                          'knownpep db': 'databases',
                          'knownpep allpep lookup': 'lookups',
                          'knownpep tryp lookup': 'lookups',
                          'biomart map': 'marts',
                          'knownpep predpi tabular': 'pipeptides',
                          }
    libtype = dset_names_libname[lib_dset_name]
    dsets = gi.libraries.show_library(galaxydata.libraries[libtype],
                                      contents=True)
    print('Select a {} dataset from {}, or enter to skip'.format(lib_dset_name,
                                                                 libtype))
    print('--------------------')
    dsets = [x for x in dsets if x['type'] == 'file']
    for ix, dset in enumerate(dsets):
        print(ix, dset['name'])
    while True:
        pick = input('Enter selection: ')
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
        return {'src': 'ld', 'id': dsets[pick]['id'],
                'galaxy_name': dsets[pick]['name']}
    return False


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


def check_workflow_inputs_ok(mod, mod_inputs, collected_inputs):
    inputs_ok = True
    for modinput in mod_inputs:
        if modinput not in collected_inputs:
            print('WARNING, workflow "{}" input "{}" not found in '
                  'outputs from earlier workflow modules. Consider fixing '
                  'before a run'.format(mod, modinput))
            inputs_ok = False
    return inputs_ok


def get_workflow_params(wf_json):
    """Should return step tool_id, name, composed_name"""
    for step in wf_json['steps'].values():
        try:
            tool_param_inputs = step['tool_inputs'].items()
        except AttributeError:
            continue
        dset_input_names = [x['name'] for x in step['inputs']]
        for input_name, input_val in tool_param_inputs:
            if input_name in dset_input_names:
                continue
            try:
                input_val = json.loads(input_val)
            except ValueError:
                # no json obj, no runtime values
                continue
            if type(input_val) == dict:
                if dict in [type(x) for x in input_val.values()]:
                    # complex input with repeats/conditional
                    for subname, subval in input_val.items():
                        composed_name = '{}|{}'.format(input_name, subname)
                        if is_runtime_param(subval, composed_name, step):
                            yield {'tool_id': step['tool_id'],
                                   'name': composed_name,
                                   'storename': input_name}
                else:
                    # simple runtime value check and fill with inputstore value
                    if is_runtime_param(input_val, input_name, step):
                        yield {'tool_id': step['tool_id'],
                               'name': input_name, 'storename': False}


def get_searchname(inputstore):
    return '{}_{}'.format(inputstore['base_searchname'],
                          inputstore['searchtype'])


def add_repeats_to_workflow_json(inputstore, wf_json):
    """Takes as input wf_json the thing the output from
    gi.workflows.export_workflow_json"""
    print('Updating set names and other repeats')
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
    print('Connecting loose step (percolator-in)...')
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
    stepname = get_stepname_or_annotation(step)
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
                        try:
                            input_val[subname] = params[stepname][composed_name]
                        except:
                            print('WARNING, RuntimeValue for tool {}, param '
                                  '{} expected, but nothing passed (possibly '
                                  'is an unneeded dataset '
                                  'though).'.format(stepname, composed_name))
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
        wf_fn = 'json_workflows/spectra_quant_isobaric_v0.2.json'
    else:
        wf_fn = 'json_workflows/spectra_quant_labelfree_v0.2.json'
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
                if type(connection) == list:
                    for multi_connection in connection:
                        if multi_connection['id'] >= first_tool_stepnr:
                            multi_connection['id'] = (multi_connection['id'] +
                                                      amount_spec_steps)
                elif connection['id'] >= first_tool_stepnr:
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
    # Connect to PSM table and QC
    for step in search_wf_json['steps'].values():
        if step['name'] == 'Process PSM table':
            step['input_connections']['lookup']['id'] = lookupstep['id']
        elif step['name'] == 'msstitch QC':
            step['input_connections']['lookup']['id'] = lookupstep['id']


def get_stepname_or_annotation(step):
    annot = step['annotation']
    return annot[:annot.index('---')] if annot else step['name']


def remove_annotated_steps(wf_json, annot_or_name):
    stepfound = True
    while stepfound:
        stepfound = False
        for step in wf_json['steps'].values():
            stepname = get_stepname_or_annotation(step)
            if annot_or_name in stepname:
                remove_step_from_wf(step['id'], wf_json, remove_connections=True)
                stepfound = True
                break


def remove_gene_steps(wf_json):
    print('Removing Genecentric steps and inputs from workflow')
    remove_annotated_steps(wf_json, 'gene table')


def remove_biomart_symbol_steps(wf_json):
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
    remove_annotated_steps(wf_json, 'symbol table')


def remove_step_from_wf(removestep_id, wf_json, remove_connections=False):
    """Removes a step, subtracts one from all step IDs after it, including
    input connections"""
    del(wf_json['steps'][str(removestep_id)])
    newsteps = {}
    for step in wf_json['steps'].values():
        if step['id'] > removestep_id:
            step['id'] -= 1
        removekeys = []
        for conkey, connection in step['input_connections'].items():
            if type(connection) == list:
                keepsubkeys = []
                for conix, multi_connection in enumerate(connection):
                    if multi_connection['id'] != removestep_id:
                        keepsubkeys.append(conix)
                    if multi_connection['id'] > removestep_id:
                        multi_connection['id'] -= 1
                if remove_connections:
                    connection = [connection[ix] for ix in keepsubkeys]
            elif connection['id'] > removestep_id:
                connection['id'] -= 1
            elif connection['id'] == removestep_id:
                removekeys.append(conkey)
        if remove_connections:
            for ck in removekeys:
                del(step['input_connections'][ck])
        newsteps[str(step['id'])] = step
    wf_json['steps'] = newsteps


def fill_in_iso(step, isokey, subkey, value):
    ts = json.loads(step['tool_state'])
    iso_ts = json.loads(ts[isokey])
    iso_ts[subkey] = value
    ts[isokey] = json.dumps(iso_ts)
    step['tool_state'] = json.dumps(ts)


def disable_isobaric_params(wf_json):
    print('Removing isobaric steps and inputs from workflow')
    for step in wf_json['steps'].values():
        if step['name'] == 'Process PSM table':
            fill_in_iso(step, 'isobaric', 'yesno', 'false')
            fill_in_iso(step, 'isobaric', 'denompatterns', '')
        elif step['name'] == 'Merge peptide or protein tables':
            ts = json.loads(step['tool_state'])
            ts['isobqcolpattern'] = json.dumps('')
            ts['nopsmcolpattern'] = json.dumps('')
            step['tool_state'] = json.dumps(ts)
        elif step['name'] in ['Create peptide table', 'Create protein table',
                              'Create gene table', 'Create symbol table']:
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
        stepname = get_stepname_or_annotation(step)
        if stepname.strip() == 'Normalization-ratio protein table':
            remove_step_from_wf(step['id'], wf_json)
            break
    disable_isobaric_params(wf_json)


def is_runtime_param(val, name, step):
    try:
        isruntime = val['__class__'] == 'RuntimeValue'
    except (KeyError, TypeError):
        return False
    else:
        if isruntime and name not in step['input_connections']:
            return True
        return False


def run_workflow(inputstore, gi):
    """Passed a workflow in inputstore, this function will create a runchain
    of tasks for celery. Will either use the existing celery task or fetch
    workflow JSON, mend and fill it and create a celery task for it.
    It then queues tasks to celery"""
    timest = datetime.strftime(datetime.now(), '%Y%m%d_%H.%M')
    inputstore['searchtype'] = inputstore['wf']['searchtype']
    inputstore['searchname'] = get_searchname(inputstore)
    inputstore['outdir'] = '{}_{}'.format(
        inputstore['searchname'].replace(' ', '_'), timest)
    inputstore['history'] = gi.histories.create_history(
        name=inputstore['searchname'])['id']
    inputstore['source_history'] = gi.histories.create_history(
        name='{}_source'.format(inputstore['searchname']))['id']
    wf_mods, inputstore['wf']['uploaded'] = {}, {}
    for module in inputstore['wf']['modules']:
        if module[0][0] != '@':
            modname, version, modtype = module[0], module[1], module[2]
            raw_json = get_versioned_module(modname, version)
            inputstore, g_id = finalize_galaxy_workflow(raw_json, modtype,
                                                        inputstore, timest, gi)
            modname, version, modtype = module[0], module[1], module[2]
            wf_mods['{}_{}'.format(modname, version)] = g_id
    miscfiles, runchain = [], []
    if inputstore['datasets']['target db']['id'] is not None:
        miscfiles.append('target db')
        miscfiles.append('decoy db')
    if miscfiles != []:
        runchain.extend([tasks.misc_files_copy.s(inputstore, miscfiles),
                         tasks.store_summary.s()])
    else:
        runchain.append(tasks.store_summary.delay(inputstore))
    runchain.extend([tasks.storage_copy_file.s(ix) for ix in
                     range(0, len(inputstore['raw']))])
    for module in inputstore['wf']['modules']:
        if module[0][0] == '@':
            runchain.append(nonwf_tasks.tasks[module[0]]['task'].s())
        else:
            modname, version, modtype = module[0], module[1], module[2]
            runchain.append(tasks.run_search_wf.s(
                wf_mods['{}_{}'.format(modname, version)]))
    runchain.append(tasks.download_results.s())
    res = chain(*runchain)
    res.delay()


def finalize_galaxy_workflow(raw_json, modtype, inputstore, timestamp, gi):
    if (modtype == 'proteingenes' and
            inputstore['datasets']['quant lookup']['id'] is None):
        specquant_wfjson = get_spectraquant_wf(inputstore)
        connect_specquant_workflow(specquant_wfjson, raw_json)
    wf_json = add_repeats_to_workflow_json(inputstore, raw_json)
    if modtype in ['proteingenes', 'proteins']:
        remove_biomart_symbol_steps(wf_json)
    if modtype == 'proteins':
        remove_gene_steps(wf_json)
    if inputstore['wf']['quanttype'] == 'labelfree':
        if modtype == 'peptides noncentric':
            remove_isobaric_from_peptide_centric(wf_json)
        if modtype in ['proteingenessymbols', 'proteingenes', 'proteins']:
            remove_isobaric_from_protein_centric(wf_json)
    print('Filling in runtime values...')
    for step in wf_json['steps'].values():
        fill_runtime_params(step, inputstore['params'])
    print('Uploading workflow...')
    wf_json['name'] = '{}_{}'.format(inputstore['searchname'], timestamp)
#            for x,y in wf_json.items():
#                print(x, y)
    uploaded = gi.workflows.import_workflow_json(wf_json)
    inputstore['wf']['uploaded'][uploaded['id']] = wf_json
    return inputstore, uploaded['id']
