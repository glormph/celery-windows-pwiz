import sys
import os
import argparse
from celery import chain

from tasks.galaxy import galaxydata
from tasks.galaxy import tasks
from tasks.galaxy import util
from tasks import config


TESTING_NO_CLEANUP = True


def main():
    inputstore = {'params': {},
                  'galaxy_url': config.GALAXY_URL,
                  }
    inputs = {name: {'src': 'hda', 'id': None} for name in
              get_flatfile_names_inputstore()}
    inputs.update({name: {'src': 'hdca', 'id': None} for name in
                   get_collection_names_inputstore()})
    inputstore['datasets'] = inputs
    parse_commandline(inputstore)
    gi = util.get_galaxy_instance(inputstore)
    if inputstore['run'] == 'show':
        for num, wf in enumerate(get_workflows()):
            modules = get_modules_for_workflow(wf['modules'])
            tasks.check_modules(gi, modules)
            print('{}  -  {}'.format(num, wf['name']))
    else:
        #inputstore['datasets']['spectra']
        output_dset_names = get_output_dsets()
        download_dsets = {name: inputs[name] for name in output_dset_names}
        for name, dl_dset in download_dsets.items():
            outname = '{}'.format(output_dset_names[name])
            outdir = inputstore['searchname'].replace(' ' , '_')
            dl_dset['download_state'] = False
            dl_dset['download_dest'] = os.path.join(inputstore['outshare'],
                                                    outdir, outname)
        inputstore['output_dsets'] = download_dsets
        inputstore['wf'] = [get_workflows()[num]
                            for num in inputstore['wf_num']]
        run_workflow(inputstore)


def run_workflow(inputstore):
    """Runs a wf as specified in inputstore var"""
    runchain = [tasks.tmp_create_history.s(inputstore),
                tasks.tmp_put_files_in_collection.s(),
                tasks.check_dsets_ok.s()]
    if (inputstore['run'] and len(inputstore['wf']) == 1
            and inputstore['rerun_his'] is None):
        # runs a single workflow composed of some modules
        inputstore['modules'] = get_modules_for_workflow(
            inputstore['wf'][0]['modules'])
        runchain.extend([tasks.tmp_prepare_run.s()])
        runchain.extend([tasks.run_workflow_module.s(mod_id)
                         for mod_id in inputstore['modules']])
    elif inputstore['run'] and len(inputstore['wf']) == 2:
        # run two workflows with a history transition tool in between
        firstwf_mods = get_modules_for_workflow(inputstore['wf'][0]['modules'])
#        [get_modules()[m_name] for m_name
#                         in inputstore['wf'][0]['modules']]
        second_wf_mods = get_modules_for_workflow(
            inputstore['wf'][1]['modules'])
        #second_wf_mods = [get_modules()[m_name] for m_name
        #                  in inputstore['wf'][1]['modules']]
        inputstore['modules'] = firstwf_mods + second_wf_mods
        runchain.extend([tasks.tmp_prepare_run.s()])
        runchain.extend([tasks.run_workflow_module.s(mod_id)
                         for mod_id in firstwf_mods])
        runchain.extend([tasks.reuse_history.s()])
        runchain.extend([tasks.run_workflow_module.s(mod_id)
                         for mod_id in second_wf_mods])
    elif inputstore['run'] and inputstore['rerun_his']:
        # runs one workflow with a history to reuse from
        inputstore['history'] = inputstore['rerun_his']
        inputstore['modules'] = get_modules_for_workflow(
            inputstore['wf'][0]['modules'])
        runchain.extend([tasks.reuse_history.s()])
        runchain.extend([tasks.run_workflow_module.s(mod_id)
                         for mod_id in inputstore['modules']])
    else:
        print('Not quite clear what you are trying to do here, '
              'would you like to --show workflows, run a vardb, or a normal'
              ' search?')
        sys.exit(1)
    runchain.extend([tasks.zip_dataset.s(), tasks.download_result.s(),
                     tasks.cleanup.s()])
    #print(runchain)
    #sys.exit()
    res = chain(*runchain)
    res.delay()


def parse_commandline(inputstore):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-u', dest='user')
    parser.add_argument('--outshare', dest='outshare')
    parser.add_argument('--show', dest='show', action='store_const',
                        default=False, const=True)
    parser.add_argument('--reuse-history', dest='reuse_history')
    parser.add_argument('-w', dest='analysisnr', type=int, nargs=True)
    parser.add_argument('--sourcehists', dest='sourcehistories', nargs='+')
    parser.add_argument('-d', dest='target_db')
    parser.add_argument('-m', dest='modifications')
    parser.add_argument('--name', dest='searchname')
    parser.add_argument('--mart', dest='biomart_map')
    parser.add_argument('--setnames', dest='setnames', nargs='+')
    parser.add_argument('--setpatterns', dest='setpatterns', nargs='+')
    parser.add_argument('--isobtype', dest='isobtype', default=None)
    parser.add_argument('--denominators', dest='denominators', nargs='+')
    parser.add_argument('--ppool-ids', dest='perco_ids', nargs='+')
    parser.add_argument('--ppool-size', dest='ppoolsize', default=8)
    parser.add_argument('--fastadelim', dest='fastadelim', type=str)
    parser.add_argument('--genefield', dest='genefield', type=int)
    parser.add_argument('--knownproteins', dest='knownpep_db')
    args = parser.parse_args(sys.argv[1:])
    inputstore['user'] = args.user
    inputstore['apikey'] = config.USERS[args.user][1]
    inputstore['outshare'] = args.outshare
    inputstore['sourcehis'] = args.sourcehistories
    if args.show:
        inputstore['run'] = 'show'
    else:
        inputstore['run'] = True
    for name in inputstore['datasets']:
        parsename = name.replace(' ', '_')
        if hasattr(args, parsename) and getattr(args, parsename) is not None:
            inputstore['datasets'][name]['id'] = getattr(args, parsename)
    for param in ['setnames', 'setpatterns', 'isobtype', 'genefield',
                  'perco_ids', 'ppoolsize', 'fastadelim']:
        if getattr(args, param) is not None:
            inputstore['params'][param] = getattr(args, param)
    if args.denominators is not None:
        inputstore['params']['denominators'] = ' '.join(args.denominators)
    inputstore['searchname'] = args.searchname
    inputstore['wf_num'] = args.analysisnr
    inputstore['rerun_his'] = args.reuse_history


def get_modules_for_workflow(wf_mods):
    return [(galaxydata.wf_modules[m_name], m_name) for m_name in wf_mods]


def get_flatfile_names_inputstore():
    return galaxydata.flatfile_names


def get_collection_names_inputstore():
    return galaxydata.collection_names


def get_output_dsets():
    return galaxydata.download_data_names


def get_workflows():
    return galaxydata.workflows


if __name__ == '__main__':
    main()
