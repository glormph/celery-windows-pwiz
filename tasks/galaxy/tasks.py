import os
import sys
import json
from datetime import datetime
from time import sleep
from bioblend import galaxy
from celery.result import AsyncResult

from tasks import config, dbaccess
from tasks.galaxy.util import get_galaxy_instance
from tasks.galaxy import galaxydata
from celeryapp import app


def get_library_dsets(gi):
    dset_names_libname = {'target db': 'databases', 'biomart map': 'databases',
                          'modifications': 'modifications'}
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
                print('Please enter a number corresponding to a dataset or enter')
                continue
            break
        if pick != '':
            output[name] = {'src': 'ldda', 'id': dsets[pick]['id'],
                            'galaxy_name': dsets[pick]['name']}
    return output
        

@app.task(queue=config.QUEUE_GALAXY_TOOLS)
def tmp_import_file_to_history(mzmlfile, inputstore):
    print('Importing {} to galaxy history {}'.format(mzmlfile,
                                                     inputstore['history']))
    gi = get_galaxy_instance(inputstore)
    dset = gi.tools.upload_from_ftp(mzmlfile,
                                    inputstore['history'])['outputs']
    print('File {} imported'.format(mzmlfile))
    return dset[0]['name'], dset[0]['id']


@app.task(queue=config.QUEUE_GALAXY_TOOLS)
def import_file_to_history(mzmlfile_id, mzmlfile, inputstore):
    print('Importing {} to galaxy history {}'.format(mzmlfile,
                                                     inputstore['history']))
    gi = get_galaxy_instance(inputstore)
    dset = gi.tools.upload_from_ftp(mzmlfile, inputstore['history'])['outputs']
    print('File {} imported'.format(mzmlfile))
    dbaccess.upload_file(mzmlfile_id, dset[0]['name'], dset[0]['id'])
    return dset[0]['name'], dset[0]['id']


@app.task(queue=config.QUEUE_GALAXY_TOOLS)
def tmp_put_files_in_collection(inputstore):
    print('Putting files from source histories {} in collection in search '
          'history {}'.format(inputstore['sourcehis'], inputstore['history']))
    gi = get_galaxy_instance(inputstore)
    name_id_hdas = []
    for sourcehis in inputstore['sourcehis']:
        name_id_hdas.extend([(ds['name'], ds['id']) for ds in
                             gi.histories.show_history(sourcehis,
                                                       contents=True,
                                                       deleted=False)])
    coll_spec = {
        'name': 'spectra', 'collection_type': 'list',
        'element_identifiers': [{'name': name, 'id': g_id, 'src': 'hda'}
                                for name, g_id in name_id_hdas]}
    collection = gi.histories.create_dataset_collection(inputstore['history'],
                                                        coll_spec)
    inputstore['datasets']['spectra'] = {'src': 'hdca', 'id': collection['id']}
    return inputstore


@app.task(queue=config.QUEUE_GALAXY_TOOLS)
def put_files_in_collection(dsets, inputstore):
    task_ids = inputstore['g_import_celerytasks']
    print('Waiting for {} files to be loaded in Galaxy'.format(len(task_ids)))
    while False in [AsyncResult(x).ready() for x in task_ids]:
        sleep(2)  # TODO change to 60
    if True in [AsyncResult(x).failed() for x in task_ids]:
        raise RuntimeError('Importing failed somewhere for history {}, '
                           'please check'.format(inputstore['history']))
    print('Collecting files')
    gi = get_galaxy_instance(inputstore)
    name_id_hdas = [x for x in
                    dbaccess.get_name_id_hdas(inputstore['search_dbid'])]
    coll_spec = {
        'name': 'spectra', 'collection_type': 'list',
        'element_identifiers': [{'name': name, 'id': g_id, 'src': 'hda'}
                                for name, g_id in name_id_hdas]}
    collection = gi.histories.create_dataset_collection(inputstore['history'],
                                                        coll_spec)
    inputstore['mzml_collection'] = collection['id']
    dbaccess.store_collection(inputstore['search_dbid'], collection['id'],
                              [x[1] for x in name_id_hdas])
    return inputstore


def get_searchname(inputstore):
    return '{}_{}'.format(inputstore['base_searchname'], inputstore['searchtype'])


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
        
@app.task(queue=config.QUEUE_GALAXY_WORKFLOW, bind=True)
def reuse_history(self, inputstore):
    input_labels = inputstore['wf'][0]['rerun_rename_labels'].keys()
    print('Checking reusable other history for datasets for '
          'input steps {}'.format(input_labels))
    inputstore['searchname'] = get_searchname(inputstore)
    # TODO launch download chain in this task on old history, so user will
    # not have to wait for download before new search is run. Useful in case
    # long slow downloads.
    gi = get_galaxy_instance(inputstore)
    try:
        update_inputstore_from_history(gi, inputstore['datasets'],
                                       input_labels, inputstore['history'],
                                       'reuse_history')
        create_history(inputstore, gi)
    except Exception as e:
        self.retry(countdown=60, exc=e)
    reuse_datasets = {}
    for label, newlabel in inputstore['wf'][0]['rerun_rename_labels'].items():
        if newlabel:
            reuse_datasets[newlabel] = inputstore['datasets'].pop(label)
    inputstore['datasets'] = initialize_datasets()
    inputstore['datasets'].update(reuse_datasets)
    inputstore['current_wf'] += 1
    return inputstore


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW, bind=True)
def run_workflow_module(self, inputstore, module_uuid):
    print('Getting workflow module {}'.format(module_uuid))
    gi = get_galaxy_instance(inputstore)
    module = inputstore['g_modules'][module_uuid]
    input_labels = get_input_labels(module)
    try:
        update_inputstore_from_history(gi, inputstore['datasets'],
                                       input_labels,
                                       inputstore['history'], 
                                       module['name'])
    except:
        self.retry(countdown=60)
    mod_inputs = get_input_map(module, inputstore['datasets'])
    mod_params = get_param_map(module, inputstore)
    print('Invoking workflow {} with id {}'.format(module['name'],
                                                   module['id']))
    try:
        gi.workflows.invoke_workflow(module['id'], inputs=mod_inputs,
                                     params=mod_params,
                                     history_id=inputstore['history'])
    except:
        self.retry(countdown=60)
    print('Workflow invoked')
    return inputstore


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW, bind=True)
def get_datasets_to_download(self, inputstore):
    gi = get_galaxy_instance(inputstore)
    output_names = get_output_dsets(inputstore['wf'][inputstore['current_wf']])
    download_dsets = {name: inputstore['datasets'][name] 
                      for name in output_names}
    for name, dl_dset in download_dsets.items():
        outname = '{}'.format(output_dset_names[name])
        outdir = '{}'.format(inputstore['searchname'].replace(' ' , '_'))
        dl_dset.update({'download_state': False, 'download_id': False,
                        'download_dest': os.path.join(inputstore['outshare'],
                                                      outdir, outname)})
    inputstore['output_dsets'] = download_dsets
    try:
        update_inputstore_from_history(gi, inputstore['output_dsets'],
                                       inputstore['output_dsets'].keys(),
                                       inputstore['history'], 'download')
    except Exception as e:
        self.retry(countdown=60, exc=e)
    print('Found datasets to download, {}'.format(download_dsets))
    return inputstore


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW, bind=True)
def wait_for_completion(self, inputstore):
    """Waits for all output data to be finished before continuing with 
    download steps"""
    print('Wait for completion of datasets to download')
    workflow_ok = True
    gi = get_galaxy_instance(inputstore)
    while workflow_ok and False in [x['download_id'] for x in
                                    inputstore['output_dsets'].values()]:
        print('Datasets not ready yet, checking')
        try:
            workflow_ok = check_outputs_workflow_ok(gi, inputstore)
        except Exception as e:
            self.retry(countdown=60, exc=e)
        sleep(60)
    if workflow_ok:
        print('Datasets ready for downloading in history {}'.format(inputstore['history']))
        return inputstore
    else:
        raise RuntimeError('Output datasets are in error or deleted state!')


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW)
def cleanup(inputstore):
    #gi = get_galaxy_instance(inputstore)
    # TODO implement
    pass
    # removes analysis history, mzMLs will be left on disk bc they will be in
    # another history


def get_download_task_chain(inputstore=False):
    if inputstore:
        tasks = [get_datasets_to_download.s(inputstore)]
    else:
        tasks = [get_datasets_to_download.s()]
    tasks.extend([wait_for_completion.s(), download_result.s(),
                  write_report.s()])
    return tasks


@app.task(queue=config.QUEUE_STORAGE, bind=True)
def download_result(self, inputstore):
    """Downloads both zipped collections and normal datasets"""
    print('Got command to download results to disk from Galaxy for history '
          '{}'.format(inputstore['history']))
    gi = get_galaxy_instance(inputstore)
    for dset in inputstore['output_dsets'].values():
        dirname = os.path.dirname(dset['download_dest'])
        if not os.path.exists(dirname) or not os.path.isdir(dirname):
            os.makedirs(dirname)
        try:
            gi.datasets.download_dataset(dset['download_id'],
                                         file_path=dset['download_dest'],
                                         use_default_filename=False)
        except:
            self.retry(countdown=60)
    print('Finished downloading results to disk for history '
          '{}'.format(inputstore['history']))
    return inputstore


@app.task(queue=config.QUEUE_STORAGE)
def write_report(inputstore):
    print('Writing report file for history {}'.format(inputstore['history']))
    report = {'name': inputstore['searchname'],
              'date': datetime.now().strftime('%Y%m%d'),
              'workflows': [wf['name'] for wf in inputstore['wf']],
              'workflow_modules': inputstore['module_uuids'],
              'galaxy history': inputstore['history'],
              'input parameters': inputstore['params'],
              'reused galaxy history': inputstore['rerun_his'],
              'datasets': inputstore['datasets'],
              }
              
    reportfile = os.path.join(inputstore['outshare'], 
                              inputstore['searchname'].replace(' ', '_'),
                              'report.json')
    with open(reportfile, 'w') as fp:
        json.dump(report, fp, indent=2)


def initialize_datasets():
    """(Re)fills inputstore with empty dict of datasets which are to be 
    made by Galaxy"""
    inputs = {name: {'src': 'hda', 'id': None} for name in
              get_flatfile_names_inputstore()}
    inputs.update({name: {'src': 'hdca', 'id': None} for name in
                   get_collection_names_inputstore()})
    return inputs


def check_outputs_workflow_ok(gi, inputstore):
    """Checks if to-download datasets in workflow are finished, sets their API
    ID as download_id if they are ready, returns workflow_ok status in case
    they are deleted/crashed (False) or not (True)"""
    for name, dset in inputstore['output_dsets'].items():
        if dset['download_id'] is not False:
            # already checked this dataset
            continue
        download_id = dset['id']
        print('Checking dset success for {} {}'.format(download_id, name))
        dset_success = check_dset_success(gi, download_id)
        if dset_success == 'ready':
            dset['download_id'] = download_id
        elif not dset_success:
            # Workflow crashed or user intervened, abort downloading
            return False
    return True


def check_dset_success(gi, dset_id):
        dset_info = gi.datasets.show_dataset(dset_id)
        if dset_info['state'] == 'ok' and not dset_info['deleted']:
            print('Dataset {} ready'.format(dset_id))
            return 'ready'
        elif dset_info['state'] == 'error' or dset_info['deleted']:
            # Workflow crashed or user intervened, abort downloading
            print('WARNING! Dataset {}: {} state is '
                  '{}'.format(dset_id, dset_info['name'], dset_info['state']))
            return False
        elif dset_info['state'] == 'paused':
            print('WARNING! Dataset {}: {} paused'.format(dset_id, 
                                                          dset_info['name']))
        else:
            print('Dataset {} not ready yet'.format(dset_id))
        return True


def get_input_labels(wf):
    names = []
    for wfinput in wf['inputs'].values():
        names.append(wfinput['label'])
    return names


def check_inputs_ready(datasets, inputnames, modname):
    print('Checking inputs {} for module {}'.format(inputnames, modname))
    ready, missing = True, []
    for name in inputnames:
        if datasets[name]['id'] is None:
            missing.append(name)
            ready = False
    if not ready:
        print('Missing inputs for module {}: '
              '{}'.format(modname, ', '.join(missing)))
    else:
        print('All inputs found for module {}'.format(modname))
    return ready


def dset_usable(dset):
    state_ok = True
    if 'state' in dset and dset['state'] == 'error':
        state_ok = False
    if dset['deleted'] or not state_ok:
        return False
    else:
        return True


def update_inputstore_from_history(gi, datasets, dsetnames, history_id, modname):
    print('Getting history contents')
    while not check_inputs_ready(datasets, dsetnames, modname):
        his_contents = gi.histories.show_history(history_id, contents=True,
                                                 deleted=False)
        for dset in his_contents:
            if not dset_usable(dset): 
                continue
            name = dset['name']
            if name in dsetnames and datasets[name]['id'] is None:
                print('found dset {}'.format(name))
                if datasets[name]['src'] == 'hdca':
                    datasets[name]['id'] = get_collection_id_in_his(his_contents,
                                                                      name, gi)
                elif datasets[name]['src'] == 'hda':
                    datasets[name]['id'] = dset['id']
        sleep(10)


def get_input_map(module, inputstore):
    inputmap = {}
    for modinput in module['inputs'].values():
        inputmap[modinput['uuid']] = {
            'id': inputstore[modinput['label']]['id'],
            'src': inputstore[modinput['label']]['src'],
        }
    return inputmap


def get_param_map(module, inputstore):
    parammap = {}
    for modstep in module['steps'].values():
        try:
            tool_param_inputs = modstep['tool_inputs'].items()
        except AttributeError:
            continue
        for input_name, input_val in tool_param_inputs:
            try:
                input_val = json.loads(input_val)
            except ValueError:
                # no json obj, no runtime values
                continue
            if type(input_val) == dict:
                check_and_fill_runtime_param(input_val, input_name, modstep,
                                             parammap, inputstore)
    return parammap


def check_and_fill_runtime_param(input_val, name, modstep, parammap,
                                 inputstore):
    if dict in [type(x) for x in input_val.values()]:
        # complex input with repeats/conditional
        for subname, subval in input_val.items():
            composed_name = '{}|{}'.format(name, subname)
            if is_runtime_param(subval, composed_name, modstep):
                fill_runtime_param(parammap, inputstore, composed_name,
                                   modstep, name)
    else:
        # simple runtime value check and fill with inputstore value
        if is_runtime_param(input_val, name, modstep):
            fill_runtime_param(parammap, inputstore, name, modstep)


def get_collection_id_in_his(his_contents, name, gi):
    print('Trying to find collection ID belonging to dataset {}'.format(name))
    labelfound = False
    for dset in his_contents:
        if dset['name'] == name:
            labelfound = True
        if labelfound and dset['type'] == 'collection':
            dcol = gi.histories.show_dataset_collection(dset['history_id'],
                                                        dset['id'])
            if set([x['object']['name'] for x in dcol['elements']]) == {name}:
                print('Correct, using {} id {}'.format(dset['name'],
                                                       dset['id']))
                return dset['id']
    print('No matching collection in history (yet)')
    return None


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


def fill_runtime_param(parammap, inputstore, name, step, storename=False):
    if not storename:
        storename = name
    try:
        paramval = inputstore['params'][storename]
    except KeyError:
        print('WARNING! no input param found for name {}'.format(name))
    else:
        parammap[step['tool_id']] = {'param': name, 'value': paramval}


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW, bind=True)
def check_dsets_ok(self, inputstore):
    # FIXME have to check datasets are ok, no history clean bc it doesnt exist
    # yet
    print('Not currently checking dsets are ok... please implement me!')
    return inputstore


def create_history(inputstore, gi):
    print('Creating new history for: {}'.format(inputstore['searchname']))
    history = gi.histories.create_history(name=inputstore['searchname'])
    inputstore['history'] = history['id']


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW, bind=True)
def tmp_prepare_run(self, inputstore):
    gi = get_galaxy_instance(inputstore)
    check_modules(gi, inputstore['module_uuids'])
    try:
        run_prep_tools(gi, inputstore)
    except Exception as e:  # FIXME correct Galaxy error here
        self.retry(countdown=60, exc=e)
    return inputstore


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW, bind=True)
def tmp_create_history(self, inputstore):
    gi = get_galaxy_instance(inputstore)
    try:
        create_history(inputstore, gi)
    except:  # FIXME correct Galaxy error here
        self.retry(countdown=60)
    return inputstore


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW, bind=True)
def prepare_run(self, inputstore, is_workflow=True):
    inputstore['search_dbid'] = dbaccess.init_search(
        inputstore['searchname'], inputstore['wf_id'], inputstore['mzml_ids'])
    gi = get_galaxy_instance(inputstore)
    if is_workflow:
        check_modules(gi, inputstore['module_uuids'])
    try:
        create_history(inputstore, gi)
        if is_workflow:
            run_prep_tools(gi, inputstore)
    except:  # FIXME correct Galaxy error here
        self.retry(countdown=60)
    return inputstore


def run_prep_tools(gi, inputstore):
    """Runs expdata (to be deprecated) and mslookup spectra. Not in normal WF
    because they need a repeat param setnames passed to them, not yet possible
    to call on WF API"""
    spectra = {'src': 'hdca', 'id': inputstore['datasets']['spectra']['id']}
    expdata_inputs = {'percopoolsize': inputstore['params']['ppoolsize'],
                      'spectra': spectra}
    if 'isobtype' in inputstore['params']:
        expdata_inputs['isoquant'] = inputstore['params']['isobtype']
    for count, (set_id, set_name) in enumerate(
            zip(inputstore['params']['setpatterns'],
                inputstore['params']['setnames'])):
        expdata_inputs['pools_{}|set_identifier'.format(str(count))] = set_id
        expdata_inputs['pools_{}|set_name'.format(str(count))] = set_name
    for count, pp_id in enumerate(inputstore['params']['perco_ids']):
        expdata_inputs['percopoolids_{}|ppool_identifier'.format(str(count))] = pp_id
    print('Running sample pool tool')
    pooltool = gi.tools.get_tools(tool_id='experiment_data')[0]
    expdata = gi.tools.run_tool(inputstore['history'], pooltool['id'],
                                tool_inputs=expdata_inputs)['outputs'][0]
    gi.histories.update_dataset(expdata['history_id'], expdata['id'],
                                name='expdata')
    print('Running lookup spectra tool')
    mslookuptool = gi.tools.get_tools(tool_id='mslookup_spectra')[0]
    speclookup = gi.tools.run_tool(inputstore['history'], mslookuptool['id'],
                                   tool_inputs=expdata_inputs)['outputs'][0]
    gi.histories.update_dataset(speclookup['history_id'], speclookup['id'],
                                name='spectra lookup')


def check_modules(gi, modules):
    deleted_error = False
    galaxy_modules = {}
    # FIXME not have distributed module UUIDs bc you need to distribute them
    # No need for github update every time. Doing this now.
    remote_modules = {mod['latest_workflow_uuid']: mod
                      for mod in gi.workflows.get_workflows()}
    for mod_uuid, mod_name in modules:
        print('Checking module {}: fetching workflow for {}'.format(mod_name,
                                                                    mod_uuid))
        try:
            remote_mod_id = remote_modules[mod_uuid]['id']
        except KeyError:
            raise RuntimeError('Cannot find module "{}" with UUID {} on '
                               'galaxy server '
                               'for this user'.format(mod_name, mod_uuid))
        except galaxy.client.ConnectionError as e:
            raise RuntimeError('Connection problem when asking for module "{}"'
                               ' with UUID {} on Galaxy server. '
                               'Error was {}'.format(mod_name, mod_uuid, e))
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


def get_flatfile_names_inputstore():
    return galaxydata.flatfile_names


def get_collection_names_inputstore():
    return galaxydata.collection_names


def get_output_dsets(workflows):
    outnames = set(galaxydata.download_data_names).difference(wf['not_outputs'])
    return {k: galaxydata.download_data_names[k] for k in outnames}


