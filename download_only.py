import sys
import argparse
from celery import chain

from tasks.galaxy import galaxydata
from tasks.galaxy import tasks
from tasks import config


def main():
    inputstore = {'galaxy_url': config.GALAXY_URL,
                  }
    parse_commandline(inputstore)
    runchain = [tasks.load_previous_inputstore.s(inputstore),
                tasks.download_results.s()]
    res = chain(*runchain)
    res.delay()


def parse_commandline(inputstore):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-u', dest='user')
    parser.add_argument('--history', dest='history')
    parser.add_argument('--outdir', dest='outdir')
    args = parser.parse_args(sys.argv[1:])
    inputstore['user'] = args.user
    inputstore['history'] = args.history
    inputstore['outdir'] = args.outdir
    inputstore['apikey'] = config.USERS[args.user][1]


def get_output_dsets():
    return galaxydata.download_data_names


def get_flatfile_names_inputstore():
    return galaxydata.flatfile_names


def get_collection_names_inputstore():
    return galaxydata.collection_names


if __name__ == '__main__':
    main()
