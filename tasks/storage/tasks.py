import sys
import os
import subprocess
import shutil
import ftplib
from time import sleep
from paramiko import SSHClient, rsakey
from scp import SCPClient
from celery import states, result
from tasks import config, dbaccess
from celeryapp import app
from tasks.galaxy.tasks import import_file_to_history, put_files_in_collection

PROTEOWIZ_LOC = ('C:\Program Files\ProteoWizard\ProteoWizard '
                 '3.0.6002\msconvert.exe')
RAWDUMPS = 'C:\\rawdump'
MZMLDUMPS = 'C:\\mzmldump'
OUTBOX = 'X:'


@app.task(queue=config.QUEUE_STORAGE)
def tmp_convert_to_mzml(rawfile, inputstore):
    if sys.platform.startswith("win"):
        # Don't display the Windows GPF dialog if the invoked program dies.
        # See comp.os.ms-windows.programmer.win32
        # How to suppress crash notification dialog?, Jan 14,2004 -
        # Raymond Chen's response [1]
        import ctypes
        SEM_NOGPFAULTERRORBOX = 0x0002  # From MSDN
        ctypes.windll.kernel32.SetErrorMode(SEM_NOGPFAULTERRORBOX)
        subprocess_flags = 0x8000000  # win32con.CREATE_NO_WINDOW?
    else:
        subprocess_flags = 0
    remote_file = os.path.join(inputstore['winshare'],
                               inputstore['storage_directory'], rawfile)
    print('Received conversion command for file {0}'.format(remote_file))
    infile = copy_infile(remote_file)
    outfile = os.path.splitext(os.path.basename(infile))[0] + '.mzML'
    resultpath = os.path.join(MZMLDUMPS, outfile)
    command = [PROTEOWIZ_LOC, infile, '--filter', '"peakPicking true 2"',
               '--filter', '"precursorRefine"', '-o', MZMLDUMPS]
    process = subprocess.Popen(command, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               creationflags=subprocess_flags)
    (stdout, stderr) = process.communicate()
    if process.returncode != 0 or not os.path.exists(resultpath):
        raise RuntimeError('Error in running msconvert:\n{}'.format(stdout))
    copy_outfile(resultpath)
    os.remove(infile)
    os.remove(resultpath)
    return outfile


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


@app.task(queue=config.QUEUE_SCP)
def tmp_scp_storage(resultfn, inputstore):
    print('Copying mzML file {} to storage'.format(resultfn))
    scpkey = rsakey.RSAKey()
    scpkey.from_private_key_file(config.SCPKEYFILE)
    mzmlfile = os.path.join('/var/data/conversions', resultfn)
    dst = os.path.join(inputstore['storageshare'],
                       inputstore['storage_directory'], resultfn)
    ssh = SSHClient()
    ssh.connect(config.STORAGESERVER, username=config.SCP_LOGIN, pkey=scpkey)
    with SCPClient(ssh.get_transport()) as scp:
        scp.put(mzmlfile, dst)
    print('done with {}'.format(resultfn))
    os.remove(mzmlfile)
    return dst


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


def copy_infile(rawfile):
    dst = os.path.join(RAWDUMPS, os.path.basename(rawfile))
    print('copying file to local dumpdir')
    shutil.copy(rawfile, dst)
    return dst


def copy_outfile(outfile):
    print('copying result file to outbox')
    dst = os.path.join(OUTBOX, os.path.basename(outfile))
    shutil.copy(outfile, dst)
