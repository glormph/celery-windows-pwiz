import os
import json
import re
import subprocess
from time import sleep

from tasks import config
from tasks.galaxy.util import get_galaxy_instance
from tasks.galaxy import galaxydata
from celeryapp import app


@app.task(queue=config.QUEUE_GALAXY_TOOLS, bind=True)
def storage_copy_file(self, inputstore, number):
    """Inputstore will contain one raw file, and a galaxy history.
    Raw file will have a file_id which can be read by the DB"""
    rawdata = inputstore['raw'][number]
    print('Copy-importing {} to galaxy history '
          '{}'.format(rawdata['filename'], inputstore['source_history']))
    gi = get_galaxy_instance(inputstore)
    tool_inputs = {
        'folder': os.path.join(config.GALAXY_STORAGE_MOUNTPATH,
                               rawdata['folder']),
        'filename': rawdata['filename'],
        'transfertype': 'copy',
        'dtype': 'mzml',
    }
    dset = gi.tools.run_tool(inputstore['source_history'], 'locallink',
                             tool_inputs=tool_inputs)
    state = dset['jobs'][0]['state']
    while state in ['new', 'queued', 'running']:
        sleep(10)
        state = gi.jobs.show_job(dset['jobs'][0]['id'])['state']
    if state == 'ok':
        print('File {} imported'.format(rawdata['filename']))
        rawdata['galaxy_id'] = dset['outputs'][0]['id']
        return inputstore
    else:
        errormsg = 'Problem copying file {}'.format(rawdata['filename'])
        print(errormsg)
        self.retry(exc=errormsg)


@app.task(queue=config.QUEUE_GALAXY_TOOLS, bind=True)
def misc_files_copy(self, inputstore, filelist):
    """Inputstore will contain some files that are on storage server, which
    may be imported to search history. E.g. special DB"""
    gi = get_galaxy_instance(inputstore)
    dsets = inputstore['datasets']
    for dset_name in filelist:
        print('Copy-importing {} to galaxy history '.format(dset_name))
        tool_inputs = {
            'folder': 'databases',
            'filename': dsets[dset_name]['id'][0],
            'transfertype': 'copy',
            'dtype': dsets[dset_name]['id'][1],
        }
        dset = gi.tools.run_tool(inputstore['source_history'], 'locallink',
                                 tool_inputs=tool_inputs)
        state = wait_for_copyjob(dset, gi)
        if state == 'ok':
            print('File {} imported'.format(dsets[dset_name]['id'][0]))
            dsets[dset_name]['id'] = dset['outputs'][0]['id']
            dsets[dset_name]['src'] = 'hda'
        else:
            errormsg = ('Problem copying file '
                        '{}'.format(dsets[dset_name]['id'][0]))
            print(errormsg)
            self.retry(exc=errormsg)
    return inputstore


def wait_for_copyjob(dset, gi):
    state = dset['jobs'][0]['state']
    while state in ['new', 'queued', 'running']:
        sleep(10)
        state = gi.jobs.show_job(dset['jobs'][0]['id'])['state']
    return state


@app.task(queue=config.QUEUE_GALAXY_TOOLS, bind=True)
def local_link_file(self, inputstore, number):
    """Inputstore will contain one raw file, and a galaxy history.
    Raw file will have a file_id which can be read by the DB"""
    rawdata = inputstore['raw'][number]
    print('Softlink-importing {} to galaxy history '
          '{}'.format(rawdata['filename'], inputstore['source_history']))
    gi = get_galaxy_instance(inputstore)
    tool_inputs = {
        'folder': os.path.join(config.GALAXY_STORAGE_MOUNTPATH,
                               rawdata['folder']),
        'filename': rawdata['filename'],
        'transfertype': 'link',
        'dtype': 'mzml',
    }
    dset = gi.tools.run_tool(inputstore['source_history'], 'locallink',
                             tool_inputs=tool_inputs)['outputs'][0]['id']
    print('File {} imported'.format(rawdata['filename']))
    rawdata['galaxy_id'] = dset[0]['id']
    return inputstore


@app.task(queue=config.QUEUE_GALAXY_TOOLS, bind=True)
def tmp_import_file_to_history(self, inputstore):
    # FIXME DEPRECATE OR MAKE SURE INPUTSTORE GETS CORRECT FORMAT
    print('Importing {} to galaxy history {}'.format(inputstore['mzml'],
                                                     inputstore['history']))
    gi = get_galaxy_instance(inputstore)
    dset = gi.tools.upload_from_ftp(os.path.join(inputstore['ftpdir'],
                                                 inputstore['mzml']),
                                    inputstore['history'])['outputs']
    print('File {} imported'.format(inputstore['mzml']))
    inputstore['galaxy_dset'] = dset[0]['id']
    inputstore['galaxy_name'] = dset[0]['name']
    return inputstore


@app.task(queue=config.QUEUE_GALAXY_TOOLS)
def create_spectra_db_pairedlist(inputstore):
    print('Creating paired list of spectra and prefractionated DB')
    gi = get_galaxy_instance(inputstore)
    # have both dbs ready, do not want to create decoys for pI separated stuff
    dsets = inputstore['datasets']
    params = inputstore['params']
    speccol = get_collection_contents(gi, dsets['spectra']['history'],
                                      dsets['spectra']['id'])
    setpatterns, pipatterns = params['setpatterns'], params['strippatterns']

    def get_coll_name_id(collection):
        return ((x['object']['name'], x['object']['id'])
                for x in collection)
    for td in ['target', 'decoy']:
        # first collect pidb collection contents for each strip/set, have a
        # dict with {frnrstr: elementdict}
        pidbcol = {}
        for set_id in setpatterns:
            pidbcol[set_id] = {}
            for pipat, strip in zip(pipatterns, params['strips']):
                dbname = '{}_{}'.format(td, get_prefracdb_name(set_id,
                                                               strip['name']))
                pidbcol[set_id][pipat] = {
                    # Hardcoded fr matcher for db
                    int(re.sub('.*fr([0-9][0-9]).*', r'\1',
                               x['element_identifier'])): x
                    for x in get_collection_contents(
                        gi, dsets[dbname]['history'], dsets[dbname]['id'])}
        # now loop spectra and pair with pi-db
        elements = []
        for set_id in setpatterns:
            set_spec = [speccol[y] for y in get_filename_index_with_identifier(
                [x['element_identifier'] for x in speccol], set_id)]
            for pipattern in pipatterns:
                pispec = [(set_spec[y],
                           int(re.sub(params['fr_matcher'], r'\1',
                                      set_spec[y]['element_identifier'])))
                          for y in get_filename_index_with_identifier(
                              [x['element_identifier'] for x in set_spec],
                              pipattern)]
                for spec, frnr in pispec:
                    dbtopair = pidbcol[set_id][pipattern][frnr]['object']
                    spobj = spec['object']
                    elements.append(
                        {'name': '{}_pIDB_{}'.format(spobj['name'],
                                                     dbtopair['name']),
                         'collection_type': 'paired', 'src': 'new_collection',
                         'element_identifiers': [{'name': 'forward',
                                                  'id': spobj['id'],
                                                  'src': 'hda'},
                                                 {'name': 'reverse',
                                                  'id': dbtopair['id'],
                                                  'src': 'hda'}]})
        # FIXME TEST THIS NEW THING BEFORE DEPLOY ON SMALL LCC2 data (5fr)
        colname = 'spectra {} db'.format(td)
        collection = gi.histories.create_dataset_collection(
            inputstore['history'], {'name': colname,
                                    'collection_type': 'list:paired',
                                    'element_identifiers': elements})
        dsets[colname] = {'src': 'hdca', 'id': collection['id'],
                          'history': inputstore['history']}
    return inputstore


def collect_spectra(inputstore, gi):
    print('Putting files from source histories {} in collection in search '
          'history {}'.format(inputstore['source_history'],
                              inputstore['history']))
    name_id_hdas = []
    #for sourcehis in inputstore['datasets']['sourcehis']:
    for mzml in inputstore['raw']:
        name_id_hdas.append((mzml['filename'], mzml['galaxy_id']))
    if 'sort_specfiles' in inputstore['params']:
        name_id_hdas = sorted(name_id_hdas, key=lambda x: x[0])
    coll_spec = {
        'name': 'spectra', 'collection_type': 'list',
        'element_identifiers': [{'name': name, 'id': g_id, 'src': 'hda'}
                                for name, g_id in name_id_hdas]}
    collection = gi.histories.create_dataset_collection(inputstore['history'],
                                                        coll_spec)
    inputstore['datasets']['spectra'] = {'src': 'hdca', 'id': collection['id'],
                                         'history': inputstore['history']}
    return inputstore


def get_collection_contents(gi, history, collection_id):
    return gi.histories.show_dataset_collection(history,
                                                collection_id)['elements']


def get_filename_index_with_identifier(infiles, pool_id):
    """Returns list of indices of infiles where the indices are of filenames
    that regex-match to pool_id"""
    pool_indices = []
    for index, fn in enumerate(infiles):
        if re.search(pool_id, fn) is not None:
            pool_indices.append(index)
    return pool_indices


@app.task(queue=config.QUEUE_GALAXY_TOOLS)
def create_6rf_split_dbs(inputstore):
    print('Creating 6RF split DB')
    gi = get_galaxy_instance(inputstore)
    params, dsets = inputstore['params'], inputstore['datasets']
    update_inputstore_from_history(gi, dsets, ['peptable MS1 deltapi'],
                                   inputstore['history'],
                                   'prep predpi peptable')
    peptable_col = gi.histories.show_dataset_collection(
        dsets['peptable MS1 deltapi']['history'],
        dsets['peptable MS1 deltapi']['id'])
    peptable_ds = {x['element_identifier']: x['object']['id']
                   for x in peptable_col['elements']}
    # run grep tool on fn range column with --pipatterns
    greptool = ('toolshed.g2.bx.psu.edu/repos/bgruening/text_processing/'
                'tp_grep_tool/1.0.0')
    splitpeptables = {x: {} for x in params['setnames']}
    for setname, peptable_id in peptable_ds.items():
        pep_in = {'src': 'hda', 'id': peptable_id}
        for pipat, strip in zip(params['strippatternlist'], params['strips']):
            greppat = 'Uploaded|{}'.format(pipat)
            pipep = gi.tools.run_tool(inputstore['history'], greptool,
                                      tool_inputs={'infile': pep_in,
                                                   'url_paste': greppat})
            splitpeptables[setname][strip['name']] = {
                'src': 'hda', 'id': pipep['outputs'][0]['id']}
    # Now run 6RF split wf
    module = gi.workflows.show_workflow(galaxydata.wf_modules['6rf split'])
    # get_input_map will error on peptable shift not existing so we pass dummy
    dsets['peptable shift'] = {'src': 'hda', 'id': 'dummy peptable'}
    mod_inputs = get_input_map(module, dsets)
    pepshift_uuid = [uuid for uuid, ds in mod_inputs.items()
                     if ds['id'] == 'dummy peptable'][0]
    dsnames = []

    for strip in params['strips']:
        for setpattern, setname in zip(params['setpatterns'],
                                       params['setnames']):
            mod_inputs[pepshift_uuid] = splitpeptables[setname][
                strip['name']]
            wfparams = {'6rf_splitter':
                        {'intercept': strip['intercept'],
                         'tolerance': strip['pi_tolerance'],
                         'fr_width': strip['fr_width'],
                         'fr_amount': strip['fr_amount'],
                         'reverse': strip['reverse']}}
            replace = {'newname':
                       get_prefracdb_name(setpattern, strip['name'])}
            gi.workflows.invoke_workflow(galaxydata.wf_modules['6rf split'],
                                         inputs=mod_inputs, params=wfparams,
                                         history_id=inputstore['history'],
                                         replacement_params=replace)
            dsname = get_prefracdb_name(setpattern, strip['name'])
            for td in ['target', 'decoy']:
                fullname = '{}_{}'.format(td, dsname)
                dsnames.append(fullname)
                dsets[fullname] = {'id': None, 'src': 'hdca'}
    # wait for all datasets to finish
    update_inputstore_from_history(gi, dsets, dsnames, inputstore['history'],
                                   'Finishing 6rf split')
    return inputstore


def get_prefracdb_name(setname, stripname):
    return '{}::{}'.format(setname, stripname)


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW, bind=True)
def run_search_wf(self, inputstore, wf_id):
    print('Workflow start task: Preparing inputs for workflow '
          'module {}'.format(wf_id))
    gi = get_galaxy_instance(inputstore)
    if inputstore['datasets']['spectra']['id'] is None:
        inputstore = collect_spectra(inputstore, gi)
    wf_json = inputstore['wf']['uploaded'][wf_id]
    input_labels = get_input_labels_json(wf_json)
    try:
        update_inputstore_from_history(gi, inputstore['datasets'],
                                       input_labels,
                                       inputstore['history'],
                                       wf_json['name'])
    except:
        self.retry(countdown=60)
    mod_inputs = get_input_map_from_json(wf_json, inputstore['datasets'])
    print('Invoking workflow {} with id {}'.format(wf_json['name'], wf_id))
    try:
        gi.workflows.invoke_workflow(wf_id, inputs=mod_inputs,
                                     history_id=inputstore['history'])
    except Exception as e:
        # Workflows are invoked so requests are fast, no significant
        # risk for timeouts
        print('Problem, retrying, error was {}'.format(e))
        self.retry(countdown=60, exc=e)
    print('Workflow invoked')
    return inputstore


def get_datasets_to_download(inputstore, outpath_full, gi):
    print('Collecting datasets to download')
    download_dsets = {}
    for step in [x for wfj in inputstore['wf']['uploaded'].values()
                 for x in wfj['steps'].values()]:
        if not 'post_job_actions' in step:
            continue
        pj = step['post_job_actions']
        for pjk in pj:
            if pjk[:19] == 'RenameDatasetAction':
                nn = pj[pjk]['action_arguments']['newname']
                if nn[:4] == 'out:':
                    outname = nn[5:].replace(' ', '_')
                    download_dsets[nn] = {
                        'download_state': False, 'download_id': False,
                        'id': None, 'src': 'hda',
                        'download_dest': os.path.join(outpath_full, outname)}
    inputstore['output_dsets'] = download_dsets
    print('Defined datasets from workflow: {}'.format(download_dsets.keys()))
    update_inputstore_from_history(gi, inputstore['output_dsets'],
                                   inputstore['output_dsets'].keys(),
                                   inputstore['history'], 'download')
    print('Found datasets to download, {}'.format(download_dsets))
    return inputstore


def wait_for_completion(inputstore, gi):
    """Waits for all output data to be finished before continuing with
    download steps"""
    print('Wait for completion of datasets to download')
    workflow_ok = True
    while workflow_ok and False in [x['download_id'] for x in
                                    inputstore['output_dsets'].values()]:
        print('Datasets not ready yet, checking')
        workflow_ok = check_outputs_workflow_ok(gi, inputstore)
    if workflow_ok:
        print('Datasets ready for downloading in history '
              '{}'.format(inputstore['history']))
        return inputstore
    else:
        raise RuntimeError('Output datasets are in error or deleted state!')


def cleanup(inputstore, gi):
    gi.histories.delete_history(inputstore['history'], purge=True)
    if not inputstore['keep_source']:
        gi.histories.delete_history(inputstore['source_history'], purge=True)


@app.task(queue=config.QUEUE_STORAGE, bind=True)
def store_summary(self, inputstore):
    """Stores workflow JSON files, and other dataset choices in
    a report file"""
    print('Storing summary for search {}'.format(inputstore['outdir']))
    gi = get_galaxy_instance(inputstore)
    outpath_full = os.path.join(config.STORAGESHARE,
                                '{}_results'.format(inputstore['user']),
                                inputstore['outdir'])
    if not os.path.exists(outpath_full) or not os.path.isdir(outpath_full):
        os.makedirs(outpath_full)
    for wf_j in inputstore['wf']['uploaded'].values():
        wfname = 'workflow_{}'.format(wf_j['name'])
        with open(os.path.join(outpath_full, wfname), 'w') as fp:
            json.dump(wf_j, fp)
    summaryfn = os.path.join(outpath_full, 'summary.json')
    inputstore['summary'] = summaryfn
    summary = {'datasets': {}, 'params': inputstore['params'],
               'job_results': []}
    for name, dset in inputstore['datasets'].items():
        if 'id' in dset and dset['id'] is not None:
            summary['datasets'][name] = dset
            if dset['src'] not in ['ld', 'disk']:
                print('Dataset {} : {} neither in library nor file on disk, '
                      'probably Galaxy ID: fetching'.format(name, dset['id']))
                gname = gi.datasets.show_dataset(dset['id'])['name']
                summary['datasets'][name]['galaxy_name'] = gname
    with open(summaryfn, 'w') as fp:
        json.dump(summary, fp)
    print('Summary stored')
    return inputstore


@app.task(queue=config.QUEUE_STORAGE, bind=True)
def download_results(self, inputstore):
    """Downloads both zipped collections and normal datasets"""
    print('Got command to download results to disk from Galaxy for history '
          '{}'.format(inputstore['history']))
    gi = get_galaxy_instance(inputstore)
    outpath_full = os.path.join(config.STORAGESHARE,
                                '{}_results'.format(inputstore['user']),
                                inputstore['outdir'])
    try:
        inputstore = get_datasets_to_download(inputstore, outpath_full, gi)
        inputstore = wait_for_completion(inputstore, gi)
    except Exception as e:
        print('Problem downloading datasets, retrying in 60s. '
              'Problem message:', e)
        self.retry(countdown=60, exc=e)
    for dset in inputstore['output_dsets'].values():
        if dset['download_url'][:4] != 'http':
            dset['download_url'] = '{}{}'.format(inputstore['galaxy_url'],
                                                 dset['download_url'])
        dlcmd = ['curl', '-o', dset['download_dest'], dset['download_url']]
        print('running: {}'.format(dlcmd))
        try:
            subprocess.check_call(dlcmd)
        except BaseException as e:
            print('Problem occurred downloading: {}'.format(e))
            self.retry(countdown=60)
    print('Finished downloading results to disk for history '
          '{}. Wrapping up stdout and erasing history and '
          'source history'.format(inputstore['history']))
    write_stdouts(inputstore, outpath_full, gi)
    cleanup(inputstore, gi)
    return inputstore


def write_stdouts(inputstore, outpath_full, gi):
    if inputstore['params']['multiplextype'] is None:
        return
    hiscon = gi.histories.show_history(inputstore['history'], contents=True)
    possible_dscols = [x for x in hiscon
                       if x['name'][:20] == 'Create peptide table'
                       and x['history_content_type'] == 'dataset_collection']
    stdouts = {}
    for pepdscol in possible_dscols:
        coldsets = gi.histories.show_dataset_collection(inputstore['history'],
                                                        pepdscol['id'])
        for colds in coldsets['elements']:
            pep_job = gi.jobs.show_job(gi.datasets.show_dataset(
                colds['object']['id'])['creating_job'], full_details=True)
            if not 'medians' in pep_job['stdout']:
                break
            stdouts[colds['element_identifier']] = pep_job['stdout']
    summaryfn = os.path.join(outpath_full, 'summary.json')
    with open(summaryfn) as fp:
        report = json.load(fp)
    report['stdout'] = {'normalizing medians': stdouts}
    with open(summaryfn, 'w') as fp:
        json.dump(report, fp, indent=2)


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
        if not dset_success:
            # Workflow crashed or user intervened, abort downloading
            return False
        elif type(dset_success) == str:
            dset['download_id'] = download_id
            dset['download_url'] = dset_success
    return True


def check_dset_success(gi, dset_id):
        dset_info = gi.datasets.show_dataset(dset_id)
        if dset_info['state'] == 'ok' and not dset_info['deleted']:
            print('Dataset {} ready'.format(dset_id))
            return dset_info['download_url']
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


def update_inputstore_from_history(gi, datasets, dsetnames, history_id,
                                   modname):
    print('Getting history contents')
    while not check_inputs_ready(datasets, dsetnames, modname):
        his_contents = gi.histories.show_history(history_id, contents=True,
                                                 deleted=False)
        # FIXME reverse contents so we start with newest dsets?
        for index, dset in enumerate(his_contents):
            if not dset_usable(dset):
                continue
            name = dset['name']
            if name in dsetnames and datasets[name]['id'] is None:
                print('found dset {}'.format(name))
                datasets[name]['history'] = history_id
                if datasets[name]['src'] == 'hdca':
                    datasets[name]['id'] = get_collection_id_in_his(
                        his_contents, name, dset['id'], gi, index)
                elif datasets[name]['src'] == 'hda':
                    datasets[name]['id'] = dset['id']
        sleep(10)


def get_input_labels_json(wf):
    return [x[0] for x in get_workflow_inputs_json(wf)]


def get_workflow_inputs_json(wfjson):
    """From workflow JSON returns (name, uuid) of the input steps"""
    for step in wfjson['steps'].values():
        if (step['tool_id'] is None and step['name'] in
                ['Input dataset', 'Input dataset collection']):
            yield(step['label'], step['uuid'])


def get_workflow_inputs(wfmod):
    for modinput in wfmod['inputs'].values():
        yield (modinput['label'], modinput['uuid'])


def get_input_map_from_json(module, inputstore):
    inputmap = {}
    for label, uuid in get_workflow_inputs_json(module):
        inputmap[uuid] = {
            'id': inputstore[label]['id'],
            'src': inputstore[label]['src'],
        }
    return inputmap


def get_input_map(module, inputstore):
    inputmap = {}
    for label, uuid in get_workflow_inputs(module):
        inputmap[uuid] = {
            'id': inputstore[label]['id'],
            'src': inputstore[label]['src'],
        }
    return inputmap


def get_collection_id_in_his(his_contents, dset_name, named_dset_id, gi,
                             his_index=False, direction=False):
    """Search through history contents (passed) to find a collection that
    contains the named_dset_id. When passing direction=-1, the history will
    be searched backwards. Handy when having tools that do discover_dataset
    and populate a collection after creating it."""
    print('Trying to find collection ID belonging to dataset {}'
          'and ID {}'.format(dset_name, named_dset_id))
    if his_index:
        search_start = his_index
        direction = 1
    elif direction == -1:
        search_start = -1
    for dset in his_contents[search_start::direction]:
        if dset['type'] == 'collection':
            dcol = gi.histories.show_dataset_collection(dset['history_id'],
                                                        dset['id'])
            if named_dset_id in [x['object']['id'] for x in dcol['elements']]:
                print('Correct, using {} id {}'.format(dset['name'],
                                                       dset['id']))
                return dset['id']
    print('No matching collection in history (yet)')
    return None


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW, bind=True)
def check_dsets_ok(self, inputstore):
    # FIXME have to check datasets are ok, no history clean bc it doesnt exist
    # yet
    print('Not currently checking dsets are ok... please implement me!')
    return inputstore


def get_flatfile_names_inputstore():
    return galaxydata.flatfile_names


def get_collection_names_inputstore():
    return galaxydata.collection_names


def get_output_dsets(wf):
    return {k: galaxydata.download_data_names[k] for k in wf['outputs']}
