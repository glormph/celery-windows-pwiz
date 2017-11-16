import os
import sys
import shutil
import requests
import hashlib
import subprocess
from time import sleep
from urllib.parse import urljoin

import config
from celeryapp import app

PROTEOWIZ_LOC = ('C:\Program Files\ProteoWizard\ProteoWizard '
                 '3.0.11336\msconvert.exe')
PSCP_LOC = ('C:\Program Files\PuTTY\pscp.exe')
RAWDUMPS = 'C:\\rawdump'
MZMLDUMPS = 'C:\\mzmldump'


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


@app.task(bind=True, queue=config.QUEUE_STORAGE)
def md5_check_arrived_file(self, fnpath, servershare):
    """This will run on remote in other repo so there is no need to be no code
    in here, the task is an empty shell with only the task name"""
    return True


@app.task(queue=config.QUEUE_PWIZ1, bind=True)
def convert_to_mzml(self, fn, fnpath, servershare):
    fullpath = os.path.join(config.SHAREMAP[servershare], fnpath, fn)
    print('Received conversion command for file {0}'.format(fullpath))
    copy_infile(fullpath)
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
    infile = os.path.join(RAWDUMPS, os.path.basename(fullpath))
    resultpath = os.path.join(
        MZMLDUMPS, os.path.splitext(os.path.basename(fullpath))[0] + '.mzML')
    command = [PROTEOWIZ_LOC, infile, '--filter', '"peakPicking true 2"',
               '--filter', '"precursorRefine"', '-o', MZMLDUMPS]
    process = subprocess.Popen(command, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               creationflags=subprocess_flags)
    (stdout, stderr) = process.communicate()
    if process.returncode != 0 or not os.path.exists(resultpath):
        print('Error in running msconvert:\n{}'.format(stdout))
        self.retry()
    try:
        check_mzml_integrity(resultpath)
    except RuntimeError as e:
        cleanup_files(infile, resultpath)
        self.retry(exc=e)
    cleanup_files(infile)
    return resultpath


@app.task(queue=config.QUEUE_PWIZ1_OUT, bind=True)
def scp_storage(self, mzmlfile, rawfn_id, dsetdir, servershare, reporturl):
    print('Got copy-to-storage command, calculating MD5 for file '
          '{}'.format(mzmlfile))
    mzml_md5 = calc_md5(mzmlfile)
    print('Copying mzML file {} with md5 {} to storage'.format(
        mzmlfile, mzml_md5))
    storeserver = config.STORAGESERVER
    dstserver = os.path.join(storeserver, dsetdir).replace('\\', '/')
    dst = '{}@{}'.format(config.SCP_LOGIN, dstserver)
    try:
        subprocess.check_call([PSCP_LOC, '-i', config.PUTTYKEY, mzmlfile, dst])
    except:
        # FIXME probably better to not retry? put in dead letter queue?
        # usually when this task has probelsm it is usually related to network
        # or corrupt file, both of which are not nice to retry
        self.retry(countdown=60)
    print('Copied file, checking MD5 remotely using nested task')
    md5res = md5_check_arrived_file.delay(
        os.path.join(dsetdir, os.path.basename(mzmlfile)).replace('\\', '/'), servershare)
    while not md5res.ready():
        sleep(30)
    dst_md5 = md5res.get()
    if not dst_md5 == mzml_md5:
        print('Destination MD5 {} is not same as source MD5 {}. Retrying in 60 '
              'seconds'.format(dst_md5, mzml_md5))
        self.retry(countdown=60)
    postdata = {'rawfile_id': rawfn_id, 'task': self.request.id, 'md5': dst_md5,
                'servershare': servershare, 'path': dsetdir,
                'filename': os.path.basename(mzmlfile),
                'client_id': config.APIKEY}
    url = urljoin(config.KANTELEHOST, reporturl)
    try:
        update_db(url, postdata)
    except RuntimeError:
        self.retry()
    print('SCP copy done and removing local file {}'.format(mzmlfile))
    os.remove(mzmlfile)


def copy_infile(remote_file):
    dst = os.path.join(RAWDUMPS, os.path.basename(remote_file))
    print('copying file to local dumpdir')
    try:
        shutil.copy(remote_file, dst)
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
    if ('indexedmzML' in ','.join([str(x) for x in firstlines]) and
            'indexedmzML' in ','.join([str(x) for x in lastlines])):
        return True
    else:
        raise RuntimeError('WARNING, conversion did not result in mzML file '
                           'with proper head and tail! Retrying conversion.')
    # FIXME maybe implement iterparsing if this is not enough.


def cleanup_files(*files):
    for fpath in files:
        os.remove(fpath)
