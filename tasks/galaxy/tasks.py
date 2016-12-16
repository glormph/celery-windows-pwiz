import os
import sys
import json
import re
from collections import OrderedDict
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
                print('Please enter a number corresponding to a dataset or '
                      'enter')
                continue
            break
        if pick != '':
            output[name] = {'src': 'ld', 'id': dsets[pick]['id'],
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
def create_spectra_db_pairedlist(inputstore):
    print('Creating paired list of spectra and prefractionated DB')
    gi = get_galaxy_instance(inputstore)
    # have both dbs ready, do not want to create decoys for pI separated stuff
    dsets = inputstore['datasets']
    params = inputstore['params']
    speccol = get_collection_contents(gi, inputstore['history'],
                                      dsets['spectra']['id'])
    setpatterns, pipatterns = params['setpatterns'], params['pipatterns']
    stripnames = [x.split(':')[0] for x in params['strips']]

    def get_coll_name_id(collection):
        return ((x['object']['name'], x['object']['id'])
                for x in collection)
    for td in ['target', 'decoy']:
        # first collect pidb collection contents for each strip/set, have a
        # dict with {frnrstr: elementdict}
        pidbcol = {}
        for set_id in setpatterns:
            pidbcol[set_id] = {}
            for pipat, stripname in zip(pipatterns, stripnames):
                pidbcol[set_id][pipat] = {
                    # Hardcoded fr matcher for db
                    int(re.sub('.*fr([0-9][0-9]).*', r'\1',
                               x['element_identifier'])): x
                    for x in get_collection_contents(
                        gi, inputstore['history'], dsets['{}_{}'.format(
                            td, get_prefracdb_name(set_id, stripname))]['id'])}
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
        dsets[colname] = {'src': 'hdca', 'id': collection['id']}
    return inputstore


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
    return '{}_{}'.format(inputstore['base_searchname'],
                          inputstore['searchtype'])


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW, bind=True)
def run_metafiles2pin(self, inputstore):
    """Metafile2pin contains a repeat param which cannot be accessed
    in the WF API and also not in the tool API (latter due to a suspected
    bug."""
    print('Running metafiles2pin')
    gi = get_galaxy_instance(inputstore)
    update_inputstore_from_history(gi, inputstore['datasets'],
                                   ['msgf target', 'msgf decoy'],
                                   inputstore['history'], 'metafiles2pin task')
    tool_inputs = {'percopoolsize': inputstore['params']['ppoolsize']}
    for count, pp_id in enumerate(inputstore['params']['perco_ids']):
        param_name = 'percopoolids_{}|ppool_identifier'.format(count)
        tool_inputs[param_name] = pp_id
    td_meta = {}
    for td in ['target', 'decoy']:
        tool_inputs['searchresult'] = inputstore['datasets'][
            'msgf {}'.format(td)]
        td_meta[td] = gi.tools.run_tool(
            inputstore['history'], 'metafiles2pin_ts',
            tool_inputs=tool_inputs)['output_collections'][0]['id']
    for td in ['target', 'decoy']:
        wait_for_dynamic_collection('metafiles2pin', gi, inputstore,
                                    td_meta[td])
        inputstore['datasets']['percometa {}'.format(td)]['id'] = td_meta[td]
    return inputstore


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW, bind=True)
def merge_percobatches_to_sets(self, inputstore):
    """This tasks maps percobatches resulting from metafiles2pin and
    percolator back to percolator sets."""
    print('Running merge percolator step outside of galaxy workflow for {} in '
          'history {}'.format(inputstore['searchname'], inputstore['history']))
    gi = get_galaxy_instance(inputstore)
    # get spectra collection, with elements to get names
    specfiles = get_spectrafiles(gi, inputstore)
    inputstore['percosetbatches'] = create_percolator_tasknr_batches(
        specfiles, inputstore['params']['perco_ids'],
        inputstore['params']['ppoolsize'])
    # get split TD result collections
    update_inputstore_from_history(gi, inputstore['datasets'],
                                   ['perco batch target', 'perco batch decoy'],
                                   inputstore['history'],
                                   'merge percolator task')
    perco_t_col = gi.histories.show_dataset_collection(
        inputstore['history'],
        inputstore['datasets']['perco batch target']['id'])
    perco_d_col = gi.histories.show_dataset_collection(
        inputstore['history'],
        inputstore['datasets']['perco batch decoy']['id'])
    # Merge target and decoy separately
    for tdname, percotd in zip(['percolator pretarget', 'percolator predecoy'],
                               [perco_t_col, perco_d_col]):
        merged = []
        for setname in inputstore['params']['perco_ids']:
            # create collection of batches to run merge on
            setinfo = inputstore['percosetbatches']['psets'][setname]
            pbatches = [el for el in percotd['elements']]
            coldesc = {'collection_type': 'list', 'name': setname,
                       'element_identifiers': [{
                           'id': pbatches[batchn]['object']['id'],
                           'name': pbatches[batchn]['element_identifier'],
                           'src': 'hda'} for batchn in setinfo['batches']]}
            batchcol = gi.histories.create_dataset_collection(
                inputstore['history'], coldesc)
            # Run merge tool and append merged JSON dataset
            merged.append({'id': gi.tools.run_tool(
                inputstore['history'], 'percolator_merge',
                tool_inputs={'fm|input': {'id': batchcol['id'],
                                       'src': 'hdca'},
                             'fm|flattenormerge': 'flatten'}
                )['outputs'][0]['id'], 'src': 'hda', 'name': setname})
        # Build collection of merged datasets
        mergecoldesc = {'collection_type': 'list', 'name': tdname,
                        'element_identifiers': merged}
        mergecol = gi.histories.create_dataset_collection(
            inputstore['history'], mergecoldesc)
        inputstore['datasets'][tdname] = {'src': 'hdca', 'id': mergecol['id']}
    return inputstore


def get_collection_contents(gi, history, collection_id):
    return gi.histories.show_dataset_collection(history,
                                                collection_id)['elements']


def get_spectrafiles(gi, inputstore):
    spectra_col = get_collection_contents(
        gi, inputstore['history'], inputstore['datasets']['spectra']['id'])
    specfiles = [x['element_identifier'] for x in spectra_col]
    return specfiles


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW, bind=True)
def run_pout2mzid_on_sets(self, inputstore):
    """Runs pout2mzid for each percolator set outside of workflow to avoid
    having to do the perco-set /mzIdentML file correlation in a galaxy tool"""
    print('Running pout2mzid step outside of galaxy workflow for {} in '
          'history {}'.format(inputstore['searchname'], inputstore['history']))
    gi = get_galaxy_instance(inputstore)
    percolabels = ['perco recalc target', 'perco recalc decoy']
    update_inputstore_from_history(gi, inputstore['datasets'], percolabels,
                                   inputstore['history'], 'pout2mzid task')
    percocols = [inputstore['datasets'][x] for x in percolabels]
    specfiles = get_spectrafiles(gi, inputstore)
    specfileppools = [get_filename_index_with_identifier(specfiles, p_id)
                      for p_id in inputstore['params']['perco_ids']]
    prepoutcols = {'target': [], 'decoy': []}
    for td, perco_col in zip(['target', 'decoy'], percocols):
        allmzidfiles = [x for x in get_collection_contents(
            gi, inputstore['history'],
            inputstore['datasets']['msgf {}'.format(td)]['id'])]
        allpercofiles = [x for x in get_collection_contents(
            gi, inputstore['history'], perco_col['id'])]
        for count, (percout, ppool_index) in enumerate(zip(allpercofiles,
                                                           specfileppools)):
            mzids = [allmzidfiles[ix] for ix in ppool_index]
            coldesc = {'name': 'msgf+ mzIdentML {} ppool {}'.format(td, count),
                       'collection_type': 'list', 'element_identifiers':
                       [{'id': mzds['object']['id'],
                         'name': mzds['element_identifier'],
                         'src': 'hda'} for mzds in mzids]}
            mzidcol = gi.histories.create_dataset_collection(
                inputstore['history'], coldesc)
            poutinputs = {'percout': {'src': 'hda',
                                      'id': percout['object']['id']},
                          'mzid|multifile': True,
                          'mzid|mzids': {'src': 'hdca', 'id': mzidcol['id']},
                          'targetdecoy': td, 'schemaskip': True}
            prepoutcols[td].append(gi.tools.run_tool(inputstore['history'], 'pout2mzid',
                                 poutinputs)['output_collections'][0]['id'])
    # repackage pout2mzid set collections into one collection for all sets
    # for both target and decoy
    print('Repackaging pout2mzid collections for search {} in history '
          '{}'.format(inputstore['searchname'], inputstore['history']))
    for td in ['target', 'decoy']:
        poutcol_els = []
        for poutcolid in prepoutcols[td]:
            wait_for_dynamic_collection('pout2mzid', gi, inputstore, poutcolid)
            poutcol_els.extend([{'src': 'hda', 'id': el['object']['id'],
                                 'name': el['element_identifier']}
                                for el in gi.histories.show_dataset_collection(
                                    inputstore['history'],
                                    poutcolid)['elements']])
        poutcolname = 'pout2mzid {}'.format(td)
        poutcol = gi.histories.create_dataset_collection(
            inputstore['history'], {'name': poutcolname,
                                    'collection_type': 'list',
                                    'element_identifiers': poutcol_els})
        inputstore['datasets'][poutcolname]['id'] = poutcol['id']
    return inputstore


def get_filename_index_with_identifier(infiles, pool_id):
    """Returns list of indices of infiles where the indices are of filenames
    that regex-match to pool_id"""
    pool_indices = []
    for index, fn in enumerate(infiles):
        if re.search(pool_id, fn) is not None:
            pool_indices.append(index)
    return pool_indices


def create_percolator_tasknr_batches(filenames, ppool_ids, max_batchsize):
    """For an amount of input files, pool identifiers and a max batch size,
    return batches of files that can be percolated together"""
    if ppool_ids:
        filegroups = OrderedDict([(p_id, get_filename_index_with_identifier(
                                   filenames, p_id))
                                  for p_id in ppool_ids])
    else:
        filegroups = {1: range(len(filenames))}
    # FIXME fix for when no ppool-ids
    batch, count = [], 0
    batch_pset_info = {'specfiles': {}, 'psets': {}}
    for setname, grouped_indices in filegroups.items():
        batch_pset_info['psets'][setname] = {'batches': []}
        batch_pset_info['specfiles'].update({filenames[ix]: setname
                                             for ix in grouped_indices})
        if len(grouped_indices) > int(max_batchsize):
            batchsize = int(max_batchsize)
        else:
            batchsize = len(grouped_indices)
        for index in grouped_indices:
            batch.append(index)
            if len(batch) == int(batchsize):
                batch_pset_info['psets'][setname]['batches'].append(count)
                batch = []
                count += 1
        if len(batch) > 0:
            batch_pset_info['psets'][setname]['batches'].append(count)
            batch = []
            count += 1
    return batch_pset_info


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
def reuse_history(self, inputstore, reuse_history_id):
    input_labels = inputstore['wf']['rerun_rename_labels'].keys()
    print('Checking reusable other history for datasets for '
          'input steps {}'.format(input_labels))
    # TODO launch download chain in this task on old history, so user will
    # not have to wait for download before new search is run. Useful in case
    # long slow downloads.
    gi = get_galaxy_instance(inputstore)
    try:
        update_inputstore_from_history(gi, inputstore['datasets'],
                                       input_labels, reuse_history_id,
                                       'reuse_history')
    except Exception as e:
        self.retry(countdown=60, exc=e)
    reuse_datasets = {}
    for label, newlabel in inputstore['wf']['rerun_rename_labels'].items():
        if newlabel:
            reuse_datasets[newlabel] = inputstore['datasets'].pop(label)
    inputstore['datasets'].update(reuse_datasets)
    return inputstore


@app.task(queue=config.QUEUE_GALAXY_TOOLS)
def create_6rf_split_dbs(inputstore):
    print('Creating 6RF split DB')
    gi = get_galaxy_instance(inputstore)
    params, dsets = inputstore['params'], inputstore['datasets']
    # run WF to prep peptable to get strip shifts for 6RF split
    mod = gi.workflows.show_workflow(galaxydata.wf_modules['6rf preparation'])
    prep_inputs = get_input_map(mod, dsets)
    # FIXME this is hardcode, but follows usual standards. CHange if necessary
    # How to deal with 60a 60b, reruns etc? Ask Rui.
    strips = [galaxydata.strips[x] for x in params['strips']]
    prep_wf_params = {'toolshed.g2.bx.psu.edu/repos/bgruening/text_processing/'
                      'tp_sed_tool/1.0.0':
                      {'code': 's/{}/\\1/;'
                       's/Uploaded files/Fractions/'
                       ''.format(params['fr_matcher'])},
                      'delta_pi_peptable':
                      {'strippatterns': ' '.join(params['pipatterns']),
                       'fr_widths': ' '.join([str(x['fr_width'])
                                              for x in strips]),
                       'intercepts': ' '.join([str(x['intercept'])
                                              for x in strips]),
                       }}
    gi.workflows.invoke_workflow(mod['id'], inputs=prep_inputs,
                                 history_id=inputstore['history'],
                                 params=prep_wf_params)
    update_inputstore_from_history(gi, dsets, ['peptable MS1 deltapi'],
                                   inputstore['history'],
                                   'prep predpi peptable')
    peptable_col = gi.histories.show_dataset_collection(
        inputstore['history'], dsets['peptable MS1 deltapi']['id'])
    peptable_ds = {x['element_identifier']: x['object']['id']
                   for x in peptable_col['elements']}
    # run grep tool on fn range column with --pipatterns
    greptool = ('toolshed.g2.bx.psu.edu/repos/bgruening/text_processing/'
                'tp_grep_tool/1.0.0')
    splitpeptables = {x: {} for x in params['setnames']}
    for setname, peptable_id in peptable_ds.items():
        pep_in = {'src': 'hda', 'id': peptable_id}
        for pipat, stripname in zip(params['pipatterns'], params['strips']):
            greppat = 'Uploaded|{}'.format(pipat)
            pipep = gi.tools.run_tool(inputstore['history'], greptool,
                                      tool_inputs={'infile': pep_in,
                                                   'url_paste': greppat})
            splitpeptables[setname][stripname] = {'src': 'hda', 'id':
                                                  pipep['outputs'][0]['id']}
    # Now run 6RF split wf
    module = gi.workflows.show_workflow(galaxydata.wf_modules['6rf split'])
    # get_input_map will error on peptable shift not existing so we pass dummy
    dsets['peptable shift'] = {'src': 'hda', 'id': 'dummy peptable'}
    mod_inputs = get_input_map(module, dsets)
    pepshift_uuid = [uuid for uuid, ds in mod_inputs.items()
                     if ds['id'] == 'dummy peptable'][0]
    dsnames = []
    for stripname in params['strips']:
        strip = galaxydata.strips[stripname]
        for setpattern, setname in zip(params['setpatterns'],
                                       params['setnames']):
            mod_inputs[pepshift_uuid] = splitpeptables[setname][stripname]
            wfparams = {'6rf_splitter': {'intercept': strip['intercept'],
                                         'tolerance': strip['pi_tolerance'],
                                         'fr_width': strip['fr_width'],
                                         'fr_amount': strip['fr_amount'],
                                         'reverse': strip['reverse']}}
            replace = {'newname': get_prefracdb_name(setpattern, stripname)}
            gi.workflows.invoke_workflow(galaxydata.wf_modules['6rf split'],
                                         inputs=mod_inputs, params=wfparams,
                                         history_id=inputstore['history'],
                                         replacement_params=replace)
            dsname = get_prefracdb_name(setpattern, stripname)
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
    except Exception as e:
        # Workflows are invoked so requests are fast, no significant
        # risk for timeouts
        print('Problem, retrying, error was {}'.format(e))
        self.retry(countdown=60, exc=e)
    print('Workflow invoked')
    return inputstore


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW, bind=True)
def get_datasets_to_download(self, inputstore):
    print('Collecting dataset IDs to download')
    gi = get_galaxy_instance(inputstore)
    output_names = get_output_dsets(inputstore['wf'])
    download_dsets = {name: inputstore['datasets'][name]
                      for name in output_names}
    for name, dl_dset in download_dsets.items():
        outname = '{}'.format(output_names[name])
        outdir = '{}'.format(inputstore['searchname'].replace(' ', '_'))
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
        print('Datasets ready for downloading in history '
              '{}'.format(inputstore['history']))
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
              'workflow': inputstore['wf']['name'],
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


def wait_for_dynamic_collection(toolname, gi, inputstore, col_id):
    print('Waiting for {} to complete for search {} in history '
          '{}'.format(toolname, inputstore['searchname'],
                      inputstore['history']))
    while True:
        collection = gi.histories.show_dataset_collection(
            inputstore['history'], col_id)
        if (not collection['populated'] or
                collection['populated_state'] != 'ok'):
            break
        sleep(10)


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
                if datasets[name]['src'] == 'hdca':
                    datasets[name]['id'] = get_collection_id_in_his(
                        his_contents, index, name, dset['id'], gi)
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


def get_collection_id_in_his(his_contents, his_index, name, named_dset_id, gi):
    print('Trying to find collection ID belonging to dataset {} '
          'and ID {}'.format(name, named_dset_id))
    labelfound = False
    # FIXME certain that collection contents are ALWAYS below collection
    # in history?
    for dset in his_contents[his_index:]:
        if dset['name'] == name:
            labelfound = True
        if labelfound and dset['type'] == 'collection':
            dcol = gi.histories.show_dataset_collection(dset['history_id'],
                                                        dset['id'])
            if named_dset_id in [x['object']['id'] for x in dcol['elements']]:
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
        if step['tool_id'] not in parammap:
            parammap[step['tool_id']] = {}
        parammap[step['tool_id']].update({name: paramval})


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
    """Runs mslookup spectra. Not in normal WF
    because needs repeat param setnames passed to them, not yet possible
    to call on WF API"""
    spectra = {'src': 'hdca', 'id': inputstore['datasets']['spectra']['id']}
    set_inputs = {'spectra': spectra}
    for count, (set_id, set_name) in enumerate(
            zip(inputstore['params']['setpatterns'],
                inputstore['params']['setnames'])):
        set_inputs['pools_{}|set_identifier'.format(count)] = set_id
        set_inputs['pools_{}|set_name'.format(count)] = set_name
    mslookuptool = gi.tools.get_tools(tool_id='mslookup_spectra')[0]
    print('Running lookup spectra tool')
    speclookup = gi.tools.run_tool(inputstore['history'], mslookuptool['id'],
                                   tool_inputs=set_inputs)['outputs'][0]
    gi.histories.update_dataset(speclookup['history_id'], speclookup['id'],
                                name='spectra lookup')


def get_absent_mods(remote_mods, mods_to_check):
    absentmods = []
    for mod_uuid in mods_to_check:
        try:
            remote_mods[mod_uuid]['id']
        except KeyError:
            absentmods.append(mod_uuid)
    return absentmods


def get_remote_modules(gi):
    return {mod['latest_workflow_uuid']: mod
            for mod in gi.workflows.get_workflows()}


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


def get_flatfile_names_inputstore():
    return galaxydata.flatfile_names


def get_collection_names_inputstore():
    return galaxydata.collection_names


def get_output_dsets(wf):
    return {k: galaxydata.download_data_names[k] for k in wf['outputs']}


nonwf_galaxy_tasks = {'@pout2mzid': run_pout2mzid_on_sets,
                      '@mergepercolator': merge_percobatches_to_sets,
                      '@metafiles2pin': run_metafiles2pin,
                      }
