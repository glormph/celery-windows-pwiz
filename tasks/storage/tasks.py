import os
import ftplib
from time import sleep
from celery import states, result
from tasks import config, dbaccess
from celeryapp import app
from tasks.galaxy.tasks import import_file_to_history, put_files_in_collection
from tasks.storage.wintasks import tmp_convert_to_mzml
from tasks.storage.scp import tmp_scp_storage


@app.task(queue=config.QUEUE_FTP)
def ftp_transfer(mzml_id, server, port, ftpaccount, ftppass, ftpdir):
    fpath, fn = dbaccess.get_fullpath(mzml_id, 'storage')
    mzmlfile = os.path.join(config.STORAGESHARE, fpath, fn)
    print('Uploading file {0} to Galaxy'.format(mzmlfile))
    ftpcon = ftplib.FTP()
    ftpcon.connect(server, port)
    ftpcon.login(ftpaccount, ftppass)
    try:
        ftpcon.mkd(ftpdir)
    except ftplib.error_perm:
        # dir already exists
        print('ftp upload dir already exists')
    ftpcon.cwd(ftpdir)
    dst = os.path.split(mzmlfile)[1]
    with open(mzmlfile, 'rb') as fp:
        ftpcon.storbinary('STOR {0}'.format(dst), fp, blocksize=65535)
    print('done with {}'.format(dst))
    return os.path.join(ftpdir, dst)


@app.task(queue=config.QUEUE_FTP)
def ftp_temporary(mzmlfile, server, port, ftpaccount, ftppass, ftpdir):
    ftpcon = ftplib.FTP()
    ftpcon.connect(server, port)
    ftpcon.login(ftpaccount, ftppass)
    try:
        ftpcon.mkd(ftpdir)
    except ftplib.error_perm:
        # dir already exists
        print('ftp upload dir already exists')
    ftpcon.cwd(ftpdir)
    dst = os.path.split(mzmlfile)[1]
    with open(mzmlfile, 'rb') as fp:
        ftpcon.storbinary('STOR {0}'.format(dst), fp, blocksize=65535)
    print('done with {}'.format(dst))
    return os.path.join(ftpdir, dst)


@app.task(queue=config.QUEUE_WAIT)
def wait_for_imported(inputstore):
    print('Waiting for tasks in history {}'.format(inputstore['history']))
    while True:
        importtasks = [result.AsyncResult(task_id)
                       for task_id in inputstore['g_import_celerytasks']]
        task_states = set([task_asr.state for task_asr in importtasks])
        print('Current task states for history '
              '{}: {}'.format(inputstore['history'], task_states))
        if not task_states.difference([states.SUCCESS]):
            break
        elif states.FAILURE in task_states:
            raise RuntimeError('Failure in tasks prior to import-to-galaxy '
                               'found.')
        sleep(2)  # FIXME set to 60s, unlikely to go so fast in prod
    inputstore['mzmls'] = [as_result.get() for as_result in importtasks]
    print('All tasks ready for history {}'.format(inputstore['history']))
    print(inputstore['mzmls'])
    return inputstore
