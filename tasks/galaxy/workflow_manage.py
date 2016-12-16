import json

from celeryapp import app
from tasks import config, dbaccess
from tasks.galaxy.util import get_galaxy_instance
from tasks.galaxy import galaxydata
from tasks.galaxy import nonwf_tasks


def check_modules(gi, modules):
    deleted_error = False
    galaxy_modules = {}
    # FIXME not have distributed module UUIDs bc you need to distribute them
    # No need for github update every time. Doing this now.
    remote_modules = get_remote_modules(gi)
    print('Checking if all modules are on server')
    for mod_uuid, mod_name in modules:
        print('Checking module {}: fetching workflow for {}'.format(mod_name,
                                                                    mod_uuid))
        try:
            remote_mod_id = remote_modules[mod_uuid]['id']
        except KeyError:
            raise RuntimeError('Cannot find module "{}" with UUID {} on '
                               'galaxy server '
                               'for this user'.format(mod_name, mod_uuid))
        else:
            module = gi.workflows.show_workflow(remote_mod_id)
            if module['deleted']:
                deleted_error = True
                print('Workflow module {} with UUID {} has been '
                      'deleted on Galaxy server, please use '
                      'latest UUID'.format(module['name'], mod_uuid))
            else:
                galaxy_modules[mod_uuid] = module
    if deleted_error:
        print('Invalid workflow UUIDs have been specified, exiting')
        sys.exit(1)
    return galaxy_modules


def check_all_modules(inputstore):
    admin = {'galaxy_url': inputstore['galaxy_url'], 
             'apikey': config.ADMIN_APIKEY}
    gi_admin = get_galaxy_instance(admin)
    remote_mods = get_remote_modules(gi_admin)
    absent_mods, badwfs = {}, {}
    allmodules = {}
    for num, wf in enumerate(get_workflows()):
        modules = {x[0]: x[1] for x in
                   get_modules_for_workflow(wf['modules'])}
        absent_mods = {x: modules[x] for x in
                       get_absent_mods(remote_mods, modules.keys())}
        if not absent_mods:
            allmodules.update({mod: remote_mods[uuid] for uuid, mod 
                               in modules.items()})
            print('{}  -  {}  - OK'.format(num, wf['name']))
        else:
            badwfs[wf['name']] = absent_mods
    for wfname, mods in badwfs.items():
        print('Could not find modules on server for wf '
              '{}: {}'.format(wfname, mods))
    return allmodules


def get_workflows():
    return galaxydata.workflows


def get_modules_for_workflow(wf_mods):
    return [(galaxydata.wf_modules[m_name], m_name) for m_name in wf_mods if m_name[0] != '@']
        

def get_remote_modules(gi):
    return {mod['latest_workflow_uuid']: mod
            for mod in gi.workflows.get_workflows()}


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW, bind=True)
def transfer_workflow_modules(self, inputstore):
    print('Transferring workflow modules from admin to client account '
          'to get latest updates')
    if inputstore['apikey'] == config.ADMIN_APIKEY:
        return inputstore
    admin = {'galaxy_url': inputstore['galaxy_url'],
             'apikey': config.ADMIN_APIKEY}
    gi_admin = get_galaxy_instance(admin)
    gi = get_galaxy_instance(inputstore)
    for wf in gi.workflows.get_workflows():
        if wf['name'][:4] == 'mod:':
            gi.workflows.delete_workflow(wf['id'])
    for wf in gi_admin.workflows.get_workflows():
        if not wf['name'][:4] == 'mod:':
            continue
        print('Getting workflow from admin: {}', wf['id'], wf['name'])
        wf_json = gi_admin.workflows.export_workflow_json(wf['id'])
        wf_json['name'] = wf_json['name'].replace('(imported from API)',
                                                  '').strip()
        gi.workflows.import_workflow_json(wf_json)
    return inputstore


def get_absent_mods(remote_mods, mods_to_check):
    absentmods = []
    for mod_uuid in mods_to_check:
        try:
            remote_mods[mod_uuid]['id']
        except KeyError:
            absentmods.append(mod_uuid)
    return absentmods


def get_library_dsets(gi):
    dset_names_libname = {'target db': 'databases', 'biomart map': 'marts',
                          'modifications': 'modifications', 
                          'pipeptides known db': 'pipeptides'}
    output = {}
    for name, libtype in dset_names_libname.items():
        dsets = gi.libraries.show_library(galaxydata.libraries[libtype],
                                          contents=True)
        print('Select a {} dataset from {}, or enter to skip'.format(name,
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
            output[name] = {'src': 'ld', 'id': dsets[pick]['id'],
                            'galaxy_name': dsets[pick]['name']}
    return output


def test_workflow_specs():
    # FIXME hardcoded!
    inputstore = {'galaxy_url': config.GALAXY_URL, 'apikey': config.USERS['jorrit'][1], 'module_uuids': galaxydata.wf_modules}
    check_workflow_mod_connectivity(galaxydata.workflows[0:2], inputstore)


def check_workflow_mod_connectivity(workflows, inputstore):
    gi = get_galaxy_instance(inputstore)
    mods_inputs = {}
    mods_outputs = {}
    galaxy_modules = check_all_modules(inputstore)
    connect_ok = True
    for wf in workflows:
        print('Checking workflow connectivity for {}'.format(wf['name']))
        allinputs = (wf['lib_inputs'] + wf['required_dsets'] + 
                     wf['required_params'] + wf['not_used_tool_inputs'])
        for mod in wf['modules']:
            if mod[0] == '@' and not mod in mods_inputs:
                mods_inputs[mod] = nonwf_tasks.tasks[mod]['inputs']
                mods_inputs[mod].extend(nonwf_tasks.tasks[mod]['params'])
                mods_outputs[mod] = nonwf_tasks.tasks[mod]['outputs']
            elif not mod in mods_inputs:
                gmod = gi.workflows.show_workflow(galaxy_modules[mod]['id'])
                mods_inputs[mod] = [x[0] for x in get_workflow_inputs(gmod)]
                for param in get_workflow_params(gmod):
                    mods_inputs[mod].append(param['name'])
                mod_uuid = galaxydata.wf_modules[mod]
                wf_json = gi.workflows.export_workflow_json(mod_uuid)
                mods_outputs[mod] = get_workflow_outputs(wf_json)
            if not check_workflow_inputs_ok(mod, mods_inputs[mod],
                                            allinputs):
                connect_ok = False
            allinputs.extend(mods_outputs[mod])
    if not connect_ok:
        print('Problems in workflow connectivity')
    else:
        print('Workflows ok')


def is_runtime_param(val, name, step):
    try:
        isruntime = val['__class__'] == 'RuntimeValue'
    except TypeError:
        return False
    except KeyError:
        return False
    else:
        if isruntime and name not in step['input_steps']:
            return True
        return False


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
        for input_name, input_val in tool_param_inputs:
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


def get_workflow_inputs(wfmod):
    for modinput in wfmod['inputs'].values():
        yield (modinput['label'], modinput['uuid'])


def get_workflow_outputs(wf_json):
    outputs = []
    for step in wf_json['steps'].values():
        #if 'RenameDatasetActionoutput' in step['post_job_actions']:
        if 'post_job_actions' in step:
            for pja in step['post_job_actions']:
                try:
                    outputs.append(step['post_job_actions'][pja
                                        ]['action_arguments']['newname'])
                except KeyError:
                    pass
    return outputs 


def initialize_datasets():
    """(Re)fills inputstore with empty dict of datasets which are to be
    made by Galaxy"""
    inputs = {name: {'src': 'hda', 'id': None} for name in
              get_flatfile_names_inputstore()}
    inputs.update({name: {'src': 'hdca', 'id': None} for name in
                   get_collection_names_inputstore()})
    inputs.update({name: None for name in get_other_names_inputstore()})
    return inputs


def get_flatfile_names_inputstore():
    return galaxydata.flatfile_names


def get_collection_names_inputstore():
    return galaxydata.collection_names


def get_other_names_inputstore():
    return galaxydata.other_names
