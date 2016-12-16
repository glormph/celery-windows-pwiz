import json

from celeryapp import app
from tasks import config, dbaccess
from tasks.galaxy.util import get_galaxy_instance
from tasks.galaxy import galaxydata


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


def check_workflow_mod_connectivity():
    mods_inputs = {}
    mods_outputs = {}
    for wf in workflows:
        print('Checking workflow connectivity for {}'.format(wf['name']))
        allinputs = wf['lib_inputs'] + wf['his_inputs']
        for mod in wf['modules']:
            if mod[0] == '@' and not mod in mod_inputs:
                pass
            elif not mod in mods_inputs:
                mod_uuid = galaxydata.wf_modules[mod]
                wf_json = gi.workflows.export_workflow_json(mod_uuid)
                for param in get_workflow_params(wf_json):
                    mods_inputs[mod].append(param['name'])
                mods_inputs[mod] = get_workflow_inputs(wf_json)
                mods_outputs[mod] = get_workflow_outputs(wf_json)
            check_workflow_inputs_ok(mods_inputs[mod], allinputs)
            allinputs.extend(mods_outputs[mod])


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


def check_rerun_workflow_outputs_passed(inputstore, modules_to_skip):
    pass


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
                        composed_name = '{}|{}'.format(name, subname)
                        if is_runtime_param(subval, composed_name, modstep):
                            yield {'tool_id': step['tool_id'],
                                   'name': composed_name, 'storename': name}
                else:
                    # simple runtime value check and fill with inputstore value
                    if is_runtime_param(input_val, name, modstep):
                        yield {'tool_id': step['tool_id'],
                               'composed_name': name, 'storename': False}
   
    pass


def get_worfklow_outputs(wf_json):
    outputs = []
    for step in wf_json['steps'].values():
        if 'RenameDatasetActionoutput' in step['post_job_actions']:
            outputs.append(step['post_job_actions']['RenameDatasetActionoutput'
                                ]['action_arguments']['newname'])
    return outputs 
