import os
import sys
import argparse
from celery import chain

from tasks import config
import tasks.storage.wintasks as wt


def main():
    inputstore = {'winshare': config.WIN_STORAGESHARE}
    parse_commandline(inputstore)
    count = 0
    for directory in inputstore.pop('storage_directories'):
        inputstore['current_storage_dir'] = directory
        in_directory = os.path.join(config.BALDRICK_STORAGE_MOUNTPATH,
                                    directory)
        rawfiles = get_files_directory(in_directory, 'raw')
        for fn in rawfiles:
            inputstore['raw'] = fn
            runchain = [wt.tmp_convert_to_mzml.s(inputstore),
                        wt.tmp_scp_storage.s()]
            ch = chain(runchain)
            res = ch.delay()
        count += len(rawfiles)
    print('Queued conversion of {} files'.format(count))


def get_files_directory(directory, extension):
    rawfiles = []
    if not os.path.exists(directory):
        raise RuntimeError('Cannot find directory {0}'.format(directory))
    for fn in sorted(os.listdir(directory)):
        if os.path.splitext(fn)[-1] == '.{}'.format(extension):
            rawfiles.append(fn)
        else:
            print('Skipping non-raw file {0}'.format(fn))
    return rawfiles


def parse_commandline(inputstore):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--rawdirs', dest='rawdirs', nargs='+')
    args = parser.parse_args(sys.argv[1:])
    inputstore['storage_directories'] = args.rawdirs


if __name__ == '__main__':
    main()
