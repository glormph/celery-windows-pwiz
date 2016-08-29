import os
import sys
import json
from time import sleep
from bioblend import galaxy
from celery.result import AsyncResult

from tasks import config, dbaccess
from tasks.galaxy.util import get_galaxy_instance
from celeryapp import app


# worker module for running workflow mods
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
def tmp_put_files_in_collection(dsets, inputstore):
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
    inputstore['mzml_collection'] = collection['id']
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


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW, bind=True)
def reuse_history(self, inputstore):
    input_labels = inputstore['wf'][0]['rerun_rename_labels'].keys()
    print('Checking reusable other history for datasets for '
          'input steps {}'.format(input_labels))
    gi = get_galaxy_instance(inputstore)
    check_modules(gi, inputstore['modules'])
    try:
        update_inputstore_from_history(gi, inputstore['datasets'],
                                       input_labels, inputstore['history'])
        create_history(inputstore, gi)
    except:
        self.retry(countdown=60)
    for label, newlabel in inputstore['wf'][0]['rerun_rename_labels'].items():
        if newlabel:
            inputstore[newlabel] = inputstore.pop(label)
    return inputstore


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW, bind=True)
def run_workflow_module(self, inputstore, module_id):
    print('Getting workflow module {}'.format(module_id))
    gi = get_galaxy_instance(inputstore)
    try:
        module = gi.workflows.show_workflow(module_id)
    except:
        self.retry(countdown=60)
    input_labels = get_input_labels(module)
    while not check_inputs_ready(inputstore['datasets'], input_labels,
                                 module['name']):
        try:
            update_inputstore_from_history(gi, inputstore['datasets'],
                                           input_labels,
                                           inputstore['history'])
        except:
            self.retry(countdown=60)
        sleep(10)
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


@app.task(queue=config.QUEUE_GALAXY_TOOLS, bind=True)
def zip_dataset(self, inputstore):
    """Tar.gz creation of all collection datasets in inputstore which are
    defined as output_dset. This is a blocking task because it needs to
    wait until the zipped data is finished and not in error state"""
    # FIXME add MD5 check?
    print('Running zip_dataset on history {}'.format(inputstore['history']))
    gi = get_galaxy_instance(inputstore)
    # FIXME create package tool for collections
    try:
        ziptool = gi.tools.get_tools(tool_id='package_dataset')[0]
    except:
        self.retry(countdown=60)
    for dset in inputstore['output_dsets'].values():
        if not dset['src'] == 'hdca':
            continue
        try:
            # FIXME what happens when there is an error input data? GalaxyConn
            # Exception or what is passed? Should not retry when that happens.
            zipdset = gi.tools.run_tool(inputstore['history'], ziptool['id'],
                                        tool_inputs={'method': 'tar', 'input':
                                                     {'src': 'hdca',
                                                      'id': dset['id']}}
                                        )['outputs'][0]
        except:
            self.retry(countdown=60)
        dset['packaged'] = zipdset['id']
    workflow_ok = True
    while workflow_ok and False in [x['download_id'] for x in
                                    inputstore['output_dsets'].values()]:
        print('Datasets not ready yet, checking')
        workflow_ok = check_output_datasets_wf(gi, inputstore)
        sleep(60)
    if workflow_ok:
        return inputstore
    else:
        self.retry(countdown=60)


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW)
def zip_dataset_oldstyle(inputstore):
    """Tar.gz creation of all collection datasets in inputstore which are
    defined as output_dset"""
    # FIXME will only work  on oldstyle prod, deprecate when updated
    print('Running zip_dataset on history {}'.format(inputstore['history']))
    gi = get_galaxy_instance(inputstore)
    ziptool = gi.tools.get_tools(tool_id='package_dataset')[0]
    for dset in inputstore['output_dsets'].values():
        zipdset = gi.tools.run_tool(inputstore['history'], ziptool['id'],
                                    tool_inputs={'method': 'tar', 'input': {
                                        'src': 'hda', 'id': dset['id']}}
                                    )['outputs'][0]
        dset['packaged'] = zipdset['id']
    return inputstore


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW)
def cleanup(inputstore):
    #gi = get_galaxy_instance(inputstore)
    pass
    # removes analysis history, mzMLs will be left on disk bc they will be in
    # another history


@app.task(queue=config.QUEUE_GALAXY_RESULT_TRANSFER, bind=True)
def download_result(self, inputstore):
    """Downloads both zipped collections and normal datasets. This is a
    task which can occupy the worker for a long time, since it waits for all
    downloadable datasets to be completed"""
    print('Got command to download results to disk from Galaxy for history '
          '{}'.format(inputstore['history']))
    gi = get_galaxy_instance(inputstore)
    for dset in inputstore['output_dsets'].values():
        dirname = os.path.dirname(dset['download_dest'])
        if not os.path.exists(dirname) or not os.path.isdir(dirname):
            os.makedirs(os.path.dirname(dset['download_dest']))
        try:
            gi.datasets.download_dataset(dset['download_id'],
                                         file_path=dset['download_dest'],
                                         use_default_filename=False)
        except:
            self.retry(countdown=60)
    return inputstore


def check_output_datasets_wf(gi, inputstore):
    """Checks if to-download datasets in workflow are finished, sets their API
    ID as download_id if they are ready, returns workflow_ok status in case
    they are deleted/crashed (False) or not (True)"""
    for dset in inputstore['output_dsets'].values():
        if dset['download_id'] is not False:
            # already checked this dataset
            continue
        try:
            download_id = dset['packaged']
        except KeyError:
            download_id = dset['id']
        dset_info = gi.datasets.show_dataset(download_id)
        if dset_info['state'] == 'ok' and not dset_info['deleted']:
            dset['download_id'] = download_id
        elif dset_info['state'] == 'error' or dset_info['deleted']:
            # Workflow crashed or user intervened, abort downloading
            return False
        return True


def get_input_labels(wf):
    names = []
    for wfinput in wf['inputs'].values():
        names.append(wfinput['label'])
    return names


def check_inputs_ready(inputstore, inputnames, modname):
    print('Checking inputs {} for module {}'.format(inputnames, modname))
    ready, missing = True, []
    for name in inputnames:
        if inputstore[name]['id'] is None:
            missing.append(name)
            ready = False
    if not ready:
        print('Missing inputs for module {}: '
              '{}'.format(modname, ', '.join(missing)))
    else:
        print('All inputs found for module {}'.format(modname))
    return ready


def update_inputstore_from_history(gi, inputstore, input_names, history_id):
    print('Getting history contents')
    his_contents = gi.histories.show_history(history_id, contents=True,
                                             deleted=False)
    for dset in his_contents:
        name = dset['name']
        if name in input_names and inputstore[name]['id'] is None:
            print('found dset {}'.format(name))
            if inputstore[name]['src'] == 'hdca':
                inputstore[name]['id'] = get_collection_id_in_his(his_contents,
                                                                  name, gi)
            elif inputstore[name]['src'] == 'hda':
                inputstore[name]['id'] = dset['id']


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
        for input_name, input_val in modstep['tool_inputs'].items():
            try:
                input_val = json.loads(input_val)
            except json.decoder.JSONDecodeError:
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
def tmp_prepare_run(self, inputstore, is_workflow=True):
    gi = get_galaxy_instance(inputstore)
    if is_workflow:
        check_modules(gi, inputstore['modules'])
    try:
        create_history(inputstore, gi)
        if is_workflow:
            run_prep_tools(gi, inputstore)
    except:  # FIXME correct Galaxy error here
        self.retry(countdown=60)
    return inputstore



@app.task(queue=config.QUEUE_GALAXY_WORKFLOW, bind=True)
def prepare_run(self, inputstore, is_workflow=True):
    inputstore['search_dbid'] = dbaccess.init_search(
        inputstore['searchname'], inputstore['wf_id'], inputstore['mzml_ids'])
    gi = get_galaxy_instance(inputstore)
    if is_workflow:
        check_modules(gi, inputstore['modules'])
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
                      'spectra': spectra,
                      'isoquant': inputstore['params']['isobtype']}
    for count, (set_id, set_name) in enumerate(
            zip(inputstore['params']['setpatterns'],
                inputstore['params']['setnames'])):
        expdata_inputs['pools_{}|set_identifier'.format(str(count))] = set_id
        expdata_inputs['pools_{}|set_name'.format(str(count))] = set_name
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
    galaxy_modules = []
    # FIXME not have distributed module UUIDs bc you need to distribute them
    # No need for github update every time. Doing this now.
    for mod_id, mod_name in modules:
        #mod_name = {v: k for k, v in galaxydata.wf_modules.items()}[mod_id]
        print('Checking module {}: fetching workflow for {}'.format(mod_name,
                                                                    mod_id))
        try:
            module = gi.workflows.show_workflow(mod_id)
        except KeyError:
            raise RuntimeError('Cannot find module name {} in local module '
                               'collection. Check for spelling error or '
                               'update local collection.'.format(mod_name))
        except galaxy.client.ConnectionError:
            raise RuntimeError('Cannot find module {} with UUID {} on the '
                               'Galaxy server. Check that the correct UUID '
                               'has been passed.'.format(mod_name, mod_id))
        else:
            if module['deleted']:
                deleted_error = True
                print('Workflow module {} with UUID {} has been '
                      'deleted on Galaxy server, please use '
                      'latest UUID'.format(module['name'], mod_id))
            else:
                galaxy_modules.append(module)
    if deleted_error:
        print('Invalid workflow UUIDs have been specified, exiting')
        sys.exit(1)
    return galaxy_modules
