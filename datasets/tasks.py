import os
import sys
import requests
import hashlib
import subprocess
import psutil
import platform
from urllib.parse import urljoin
from celery.exceptions import MaxRetriesExceededError

import config
from rawstatus.tasks import get_md5
from celeryapp import app

PROTEOWIZ_LOC = ('C:\Program Files\ProteoWizard\ProteoWizard '
                 '3.0.19127.a8f2dc212\msconvert.exe')
RAWDUMPS = 'C:\\rawdump'
MZMLDUMPS = 'C:\\mzmldump'


def get_scp():
    system32dir = os.path.join(os.environ['SystemRoot'], 'SysNative' if
                               platform.architecture()[0] == '32bit' else 'System32')
    scp = [os.path.join(system32dir, 'OpenSSH', 'scp.exe'),
            '-ri', config.SSHKEY]
    if config.KNOWN_HOSTS is not None:
        scp.extend(['-o', 'UserKnownHostsFile={}'.format(config.KNOWN_HOSTS)])
    return scp


def fail_update_db(failurl, task_id):
    url = urljoin(config.KANTELEHOST, failurl)
    update_db(url, {'task': task_id, 'client_id': config.APIKEY})


def update_db(url, postdata, msg=False):
    try:
        r = requests.post(url=url, data=postdata, verify=config.CERTFILE)
        r.raise_for_status()
    except (requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError) as e:
        if not msg:
            msg = 'Could not update database: {}'
        msg = msg.format(e)
        print(msg)
        raise RuntimeError(msg)


@app.task(queue=config.QUEUE_PWIZ, bind=True)
def convert_to_mzml(self, fn, fnpath, outfile, sf_id, servershare, reporturl,
                    failurl):
    fullpath = "{}@{}:'{}'".format(config.SCP_LOGIN, config.STORAGESERVER,
            os.path.join(config.STORAGEBASE, fnpath, fn)).replace('\\', '/')
    print('Received conversion command for file {0}'.format(fullpath))
    try:
        copy_infile(fullpath)
    except RuntimeError:
        fail_update_db(failurl, self.request.id)
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
    infile = os.path.join(RAWDUMPS, fn)
    resultpath = os.path.join(MZMLDUMPS, outfile)
    command = [PROTEOWIZ_LOC, infile, '--filter', '"peakPicking true 2"',
               '--filter', '"precursorRefine"', '-o', MZMLDUMPS]
    process = subprocess.Popen(command, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               creationflags=subprocess_flags)
    try:
        (stdout, stderr) = process.communicate(timeout=3600)
    except subprocess.TimeoutExpired:
        parent = psutil.Process(process.pid)
        children = parent.children(recursive=True)
        for child in children:
            child.kill()
        dead, alive = psutil.wait_procs(children, timeout=5)
        parent.kill()
        parent.wait(5)
        cleanup_files(infile, resultpath)
        try:
            print('Conversion spent more than 1 hour, aborted and queueing for retry')
            self.retry()
        except MaxRetriesExceededError:
            print('Too many retries for this task, set to error')
            fail_update_db(failurl, self.request.id)
            raise RuntimeError
    if process.returncode != 0 or not os.path.exists(resultpath):
        print('Conversion failed, queueing for retry')
        cleanup_files(infile, resultpath)
        try:
            self.retry()
        except MaxRetriesExceededError:
            print('Too many retries. Error in running msconvert:\n{}'.format(stdout))
            fail_update_db(failurl, self.request.id)
            raise RuntimeError
    try:
        check_mzml_integrity(resultpath)
    except RuntimeError as e:
        cleanup_files(infile, resultpath)
        try:
            print('Converted file is not correct, queuing for retry')
            self.retry()
        except MaxRetriesExceededError:
            print('Too many retries. Integrity check failed', e)
            fail_update_db(failurl, self.request.id)
            raise RuntimeError('Integrity check failed multiple times')
    cleanup_files(infile)
    postdata = {'task': self.request.id, 'filename': outfile,
                'sfid': sf_id, 'client_id': config.APIKEY}
    url = urljoin(config.KANTELEHOST, reporturl)
    try:
        update_db(url, postdata)
    except RuntimeError:
        self.retry()
    print('Mzml convert subtask done and reported')
    return resultpath


@app.task(queue=config.QUEUE_PWIZ_OUT, bind=True)
def scp_storage(self, mzmlfile, sf_id, dsetdir, servershare, reporturl, failurl):
    print('Got copy-to-storage command, calculating MD5 for file '
          '{}'.format(mzmlfile))
    mzml_md5 = calc_md5(mzmlfile)
    print('Copying mzML file {} with md5 {} to storage'.format(
        mzmlfile, mzml_md5))
    storeserver = config.STORAGESERVER
    dstserver = os.path.join(storeserver, dsetdir).replace('\\', '/')
    dst = "{}@{}:'{}'".format(config.SCP_LOGIN, config.STORAGESERVER,
            os.path.join(config.STORAGEBASE, dsetdir).replace('\\', '/'))
    try:
        subprocess.check_call(get_scp() + [mzmlfile, dst])
    except Exception:
        try:
            print('Secure copy to server failed, retrying file {}'.format(mzmlfile))
            self.retry()
        except MaxRetriesExceededError:
            print('Secure copy file {} to server failed multiple times, set to error'.format(mzmlfile))
            fail_update_db(failurl, self.request.id)
            os.remove(mzmlfile)
            raise
    print('Copied file, checking MD5 remotely using nested task')
    postdata = {'sfid': sf_id, 'task': self.request.id,
                'client_id': config.APIKEY}
    url = urljoin(config.KANTELEHOST, reporturl)
    try:
        update_db(url, postdata)
    except RuntimeError:
        self.retry(countdown=60)
    print('SCP copy done and removing local file {}'.format(mzmlfile))
    os.remove(mzmlfile)
    return mzml_md5


def copy_infile(remote_file):
    dst = os.path.join(RAWDUMPS, os.path.basename(remote_file).replace("'", ''))
    print('copying file {} to local dumpdir {}'.format(remote_file, dst))
    try:
        subprocess.check_call(get_scp() + [remote_file, dst])
    except Exception as e:
        try:
            cleanup_files(dst)
        # windows specific error
        except FileNotFoundError:
            pass
        raise RuntimeError('{} -- WARNING, could not copy input {} to local '
                           'disk'.format(e, dst))
    print('Done copying file to local dumpdir')


def calc_md5(fnpath):
    hash_md5 = hashlib.md5()
    with open(fnpath, 'rb') as fp:
        for chunk in iter(lambda: fp.read(4096), b''):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def check_mzml_integrity(mzmlfile):
    """Checks if file is valid XML by parsing it"""
    # Quick and dirty with head and tail just to check it is not truncated
    with open(mzmlfile, 'rb') as fp:
        firstlines = fp.readlines(100)
        fp.seek(-100, 2)
        lastlines = fp.readlines()
    first, last = ('indexedmzML' in ','.join([str(x) for x in firstlines]),
                   'indexedmzML' in ','.join([str(x) for x in lastlines]))
    if first and last:
        return True
    else:
        print('Problem, string "indexedmzML" not found in both first and last lines of mzML'
              ', Result: first lines: {}, last lines: {}'.format(first, last))
        raise RuntimeError('WARNING, conversion did not result in mzML file '
                           'with proper head and tail!')


def cleanup_files(*files):
    for fpath in files:
        os.remove(fpath)
