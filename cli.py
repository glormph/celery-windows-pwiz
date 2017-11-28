import argparse
import sys
import os
from glob import glob
from getpass import getpass
import re

from tasks import config
from tasks.galaxy import galaxydata
import workflow_starter as wfstarter


def get_mzmls_from_dirs(sourcedirs):
    mzmls = []
    for path in sourcedirs:
        mzmls.extend([{'storagefolder': path, 'filename': fn}
                      for fn in glob('{}/*.mzML'.format(path))])
    return mzmls


def get_mzmls_from_files(sourcefiles):
    mzmls = []
    for mzmlfile in sourcefiles:
        path, fn = os.path.split(mzmlfile)
        mzmls.append({'storagefolder': path, 'filename': fn})
    return mzmls


def get_parser():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-u', dest='user')
    parser.add_argument('--show', dest='show', action='store_const',
                        default=False, const=True)
    parser.add_argument('--newuser', dest='newuser', action='store_const',
                        default=False, const=True)
    parser.add_argument('--test', dest='test', action='store_const',
                        default=False, const=True)
    parser.add_argument('--galaxy-wf', dest='galaxy_wf')
    parser.add_argument('--wftype', dest='wftype', nargs='+')
    parser.add_argument('--link', dest='link', default=False)
    parser.add_argument('--keepsource', dest='keep_source', default=False,
                        action='store_const', const=True)
    parser.add_argument('--qlookup', dest='quant_lookup')
    parser.add_argument('--spectra', dest='spectra')
    parser.add_argument('--psmcanonical', dest='psm_table_normalsearch')
    parser.add_argument('--targetdb', dest='target_db')
    parser.add_argument('--decoydb', dest='decoy_db')
    parser.add_argument('-w', dest='analysisnr', type=int)
    parser.add_argument('--sourcehists', dest='sourcehis', nargs='+')
    parser.add_argument('--sourcedirs', dest='sourcedirs', nargs='+')
    parser.add_argument('--sourcefiles', dest='sourcefiles', nargs='+')
    parser.add_argument('--sortspectra', dest='sort_specfiles', default=None,
                        action='store_const', const=True)
    parser.add_argument('--name', dest='searchname')
    parser.add_argument('--files-as-sets', dest='filesassets', default=False,
                        action='store_const', const=True)
    parser.add_argument('--setnames', dest='setnames', nargs='+')
    parser.add_argument('--setpatterns', dest='setpatterns', nargs='+')
    parser.add_argument('--isobtype', dest='multiplextype', default=None)
    parser.add_argument('--phospho', dest='phospho', default=False,
                        action='store_const', const=True)
    parser.add_argument('--instrument', dest='instrument', default=None)
    parser.add_argument('--mods', dest='modifications', nargs='+')
    parser.add_argument('--custmods', dest='custommods', nargs='+',
                        help='Specify mass_STY*_fix/opt_pos_name, where name'
                        'should be UNIMOD and pos one of n/c-term, '
                        'protein-n/c-term, any')
    parser.add_argument('--setdenominators', dest='setdenominators', nargs='+',
                        help='Provide \'setname:denominators\', and include '
                        'the quote marks')
    parser.add_argument('--strips', dest='strips', nargs='+', help='Specify '
                        'which strips have been used in split DB experiments '
                        'where DBs are pI predicted')
    parser.add_argument('--strippatterns', dest='strippatterns', nargs='+',
                        help='Regexes. Need to have same order as strips '
                        'in --strips')
    parser.add_argument('--frpattern', dest='fr_matcher',
                        help='Use this regex pattern to match fraction number '
                        'in filenames. E.g: .*fr([0-9][0-9]).*'
                        )
    parser.add_argument('--firstfrac', dest='firstfrac',
                        help='First fraction number in spectra range. Usually '
                        'is 1', type=int)
    parser.add_argument('--fdrclasses', dest='fdrclasses',
                        help='Use these regex patterns to match FDR classes in'
                        ' the FASTA DB input. Eg 2 classes, one containing '
                        "2 headers: '^lnc|^INDEL' '^CanPro'", nargs='+')
    parser.add_argument('--decoyfdrclasses', dest='decoy_fdrclasses',
                        help='Use these regex patterns to match FDR classes in'
                        ' the DECOY! FASTA DB input. Eg 2 classes, one '
                        "containing 2 headers: '^lnc|^INDEL' '^CanPro'",
                        nargs='+')
    parser.add_argument('--pipeptides', dest='pipeptides_db')
    parser.add_argument('--pipeptides-known', dest='pipeptides_known_db')
    parser.add_argument('--ppool-ids', dest='perco_ids', nargs='+')
    parser.add_argument('--ppool-size', dest='ppoolsize', default=8)
    parser.add_argument('--fastadelim', dest='fastadelim', type=str)
    parser.add_argument('--genefield', dest='genefield', type=int)
    parser.add_argument('--knownproteins', dest='knownpep_db')
    parser.add_argument('--decoyknownproteins', dest='knownpep_db_decoy')
    return parser


def parse_commandline(inputstore):
    parser = get_parser()
    args = parser.parse_args(sys.argv[1:])
    inputstore['user'] = args.user
    if args.show:
        inputstore['run'] = 'show'
        return
    elif args.newuser:
        inputstore['run'] = 'newuser'
        inputstore['password'] = getpass('Enter new user password')
        inputstore['user'] = config.USERS[args.user]
        inputstore['apikey'] = config.ADMIN_APIKEY
        return
    elif args.test:
        #inputstore['run'] = 'connectivity'
        inputstore['galaxy_wf'] = args.galaxy_wf
        inputstore['wftype'] = args.wftype
        inputstore['run'] = 'test'
        return
    else:
        inputstore['run'] = True
    for name in (wfstarter.get_flatfile_names_inputstore() +
                 wfstarter.get_collection_names_inputstore()):
        parsename = name.replace(' ', '_')
        if hasattr(args, parsename) and getattr(args, parsename) is not None:
            inputstore['datasets'][name]['id'] = getattr(args, parsename)
    if args.filesassets and (args.setnames is not None or
                             args.setpatterns is not None):
        print('Conflicting input, --files-as-sets has been passed but '
              'also set definitions. Exiting.')
        sys.exit(1)
    for param in ['setnames', 'setpatterns', 'multiplextype', 'genefield',
                  'perco_ids', 'ppoolsize', 'fastadelim', 'filesassets',
                  'modifications', 'custommods', 'instrument',
                  'fr_matcher', 'firstfrac',
                  'phospho', 'fdrclasses', 'decoy_fdrclasses',
                  'strips', 'strippatterns', 'sort_specfiles', 'keep_source',
                  'setdenominators']:
        inputstore['params'][param] = getattr(args, param)
    if args.sourcedirs is not None:
        inputstore['raw'] = get_mzmls_from_dirs(args.sourcedirs)
    elif args.sourcefiles is not None:
        inputstore['raw'] = get_mzmls_from_files(args.sourcefiles)
    inputstore['base_searchname'] = args.searchname
    inputstore['wf_num'] = args.analysisnr
    inputstore['copy_or_link'] = 'link' if args.link else 'copy'


def parse_uploadable_files(datasets, name, filetype):
    fn_or_none = datasets[name]['id']
    if fn_or_none is not None and fn_or_none[0] == '/':
        datasets[name]['id'] = (fn_or_none, filetype)
        datasets[name]['src'] = 'disk'


def parse_special_inputs(inputstore, gi):
    """Command line interface has some special inputs. Strips, filesassets,
    """
    for name, ftype in galaxydata.uploadable_files:
        parse_uploadable_files(inputstore['datasets'], name, ftype)
    params = inputstore['params']
    if params['setdenominators'] is not None:
        params['setdenominators'] = [{'setpattern': x.split(':')[0],
                                      'denoms': x.split(':')[1]}
                                     for x in params['setdenominators']]
    if params['filesassets']:
        print('Using files as sets')
        sets = [x['filename'].replace('-', '_').replace('.mzML', '')
                for x in inputstore['raw']]
        if not len(sets):
            raise RuntimeError('Cannot use files-as-sets when no filenames'
                               'have been entered')
        params['setnames'] = sets
        params['setpatterns'] = sets
    if params['perco_ids'] is None:
        print('No perco IDS entered, using setpatterns')
        params['perco_ids'] = params['setpatterns']
    if params['strips'] is not None:
        #'strips': [{'intercept': 3.5959, 'fr_width': 0.0174, 'name': '3-10'},
        #           {'intercept': 3.5478, 'fr_width': 0.0676}],
        #'strippatterns': ['IEF_37-49', 'IEF_3-10']}}
        params['strips'] = [galaxydata.strips[x] for x in params['strips']]
    # FIXME maybe these params should be tanked to the workflow_starter?
    # Repeats are also in there.
    params['Set sed'] = {'code': ';'.join(['s/.*({}).*/\\1/'.format(sm)
                                           for sm in params['setpatterns']])}
    if params['fr_matcher'] is not None:
        params['Get fraction numbers'] = {
            'code': ('s/{}/\\1/'.format(params['fr_matcher']))}
        if params['firstfrac'] is not None:
            params['Align DB fractions'] = {'frspec': params['fr_matcher'],
                                            'firstfr': params['firstfrac']}
    if params['strippatterns'] is not None:
        params['Plate sed'] = {
            'code': ';'.join(['s/.*({}).*/\\1/'.format(pm)
                              for pm in params['strippatterns']] +
                             ['s/SpecFile/Plate/'])}
    if params['fdrclasses'] is not None:
        setclasses = get_fdrclasses_setoverlap(inputstore)
        params['setfdrclasses'] = sorted([
            x['pattern'] for setfdrs in setclasses.values()
            for x in setfdrs.values()])
        inputstore['setfdrreport'] = setclasses


def get_fdrclasses_setoverlap(inputstore):
    # FIXME this is highly bound to file names. What we output is setA and a
    # suffix of length and we want those to alphabetically match
    # setA_fdrclasspatternA, setA_fdrpatB, etc
    # There will be filenames where this does not work
    setfdrclasses = {setpat: {}
                     for setpat in inputstore['params']['setpatterns']}
    for fn in [x['filename'] for x in inputstore['raw']]:
        fn = fn.replace('-', '_')
        for setpat in setfdrclasses:
            if not re.match('.*{}.*'.format(setpat), fn):
                continue
            for i in range(0, len(inputstore['params']['fdrclasses'])):
                regex = '.*({}{}).*'.format(setpat, '.' * i)
                try:
                    setfdrclasses[setpat][i]['fns'].append(fn)
                except KeyError:
                    setfdrclasses[setpat][i] = {
                        'pattern': re.sub(regex, '\\1', fn),
                        'fns': [fn]
                    }
    return setfdrclasses


def show_pattern_matchers(inputstore, gi):
    """This outputs the results of pattern matching and shows a CLI dialog
    press Enter if OK or ctr-C
    - E.g.:
        setname - filenr
        &
        set1: IEF3-10: 40 fractions
        set1: IPG37-49: 40 fractions
        &
        setname-fdr class - filenr
    """
    # FIXME history with spectra is not known
    if 'raw' in inputstore:
        sourcefiles = [fn['filename'].replace('-', '_')
                       for fn in inputstore['raw']]
    else:
        # FAIL cannot get dataset collection without history!
        #spechis = gi.datasets.
        #gi.histories.show_dataset_collection(history, dset)
        sourcefiles = []
        print('Cannot show pattern matchin becuase cannot get file names from '
              'Galaxy')
    params = inputstore['params']
    grouped_fns = {setpat: {} for setpat in params['setpatterns']}
    set_count = {setpat: 0 for setpat in params['setpatterns']}
    for fn in sourcefiles:
        found_set, found_plate, found_fr = False, False, False
        for set_pat in params['setpatterns']:
            if re.match('.*{}.*'.format(set_pat), fn):
                set_count[set_pat] += 1
                found_set = set_pat
        if not found_set:
            print('WARNING, no set found for file {}'.format(fn))
        if params['strippatterns'] is not None:
            for plate_pat in params['strippatterns']:
                print(plate_pat, fn)
                if re.match('.*{}.*'.format(plate_pat), fn):
                    found_plate = plate_pat
            if not found_plate:
                print('WARNING, no plate found for file {}'.format(fn))
        else:
            found_plate = 'no plate'
        if params['fr_matcher'] is not None:
            if re.match(params['fr_matcher'], fn):
                found_fr = re.sub(params['fr_matcher'], '\\1', fn)
            else:
                print('WARNING, no fraction found for file {}'.format(fn))
        else:
            found_fr = 'no fraction'
        try:
            grouped_fns[found_set][found_plate][found_fr] = fn
        except KeyError:
            grouped_fns[found_set][found_plate] = {found_fr: fn}
    print('####################')
    for setpat, count in set_count.items():
        print('set: {} - {} files'.format(setpat, count))
    print('----- Sets/plates/fractions -----')
    for setpat, plate in grouped_fns.items():
        for platename, fractions in plate.items():
            print('set: {} - plate: {} ---- {} fractions'.format(
                setpat, platename, len(fractions)))
    if params['fdrclasses'] is not None:
        print('----- FDR classes -----')
        for setpat, fclassdata in inputstore['setfdrreport'].items():
            #print(setpat, fclassdate)
            for index, fclass in fclassdata.items():
                print('set {} - class {}/{} - {} files match'.format(
                    setpat, index, fclass['pattern'], len(fclass['fns'])))
    print('####################')
    p = input('Does the pattern matching look ok? If not, remember that we '
              'replace dash (-) with underscore in file names (-). If yes, '
              'Press enter')
    if p == '':
        return
    else:
        print('Exiting without running')
        sys.exit()
