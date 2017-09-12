import sys
import os
import subprocess
import shutil
from hashlib import md5


from celeryapp import app
from tasks import config

# import task to chain, could poss be removed
from tasks.storage import scp


PROTEOWIZ_LOC = ('C:\Program Files\ProteoWizard\ProteoWizard '
                 '3.0.6002\msconvert.exe')
PSCP_LOC = ('C:\Program Files\Putty')
RAWDUMPS = 'C:\\rawdump'
MZMLDUMPS = 'C:\\mzmldump'
OUTBOX = 'X:'


@app.task(queue=config.QUEUE_SCP, bind=True)
def tmp_scp_storage(self, inputstore):
    mzmlfile = os.path.join(MZMLDUMPS, inputstore['mzml'])
    hashmd5 = md5()
    with open(mzmlfile, 'rb') as fp:
        for chunk in iter(lambda: fp.read(4096), b''):
            hashmd5.update(chunk)
    mzml_md5 = hashmd5.hexdigest()
    print('Copying mzML file {} with md5 {} to storage'.format(
        inputstore['mzml'], mzml_md5))
    dstfolder = os.path.join(inputstore['winshare'],
                             inputstore['current_storage_dir'])
    dst = '{}@{}'.format(config.SCP_LOGIN, dstfolder)
    try:
        subprocess.check_call([PSCP_LOC, '-i', config.PUTTYKEY, mzmlfile, dst])
    except:
        # FIXME probably better to not retry? put in dead letter queue?
        # usually when this task has probelsm it is usually related to network
        # or corrupt file, both of which are not nice to retry
        self.retry(countdown=60)
    arrived_file = os.path.join(dstfolder, os.path.basename(mzmlfile))
    try:
        with open(arrived_file, 'rb') as fp:
            for chunk in iter(lambda: fp.read(4096), b''):
                hashmd5.update(chunk)
        dst_md5 = hashmd5.hexdigest()
    except:
        # FIXME probably better to not retry? put in dead letter queue?
        # usually when this task has probelsm it is usually related to network
        self.retry(countdown=60)
    if not dst_md5 == mzml_md5:
        self.retry(countdown=60)
        return
    inputstore['mzml_md5'] = mzml_md5
    inputstore['mzml_path'] = dst
    print('done with {}'.format(inputstore['mzml']))
    os.remove(mzmlfile)
    return inputstore


@app.task(bind=True, queue=config.QUEUE_CONVERSION)
def tmp_convert_to_mzml(self, inputstore):
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
                               inputstore['current_storage_dir'],
                               inputstore['raw'])
    print('Received conversion command for file {0}'.format(remote_file))
    try:
        infile = copy_infile(remote_file)
    except Exception:
        try:
            cleanup_files(infile)
        except FileNotFoundError:
            pass
        print('{} -- WARNING, could not copy input {} to local '
              'disk'.format(e, remote_file))
        self.retry(exc=e, countdown=60)
    outfile = os.path.splitext(os.path.basename(infile))[0] + '.mzML'
    inputstore['mzml'] = outfile
    resultpath = os.path.join(MZMLDUMPS, outfile)
    command = [PROTEOWIZ_LOC, infile, '--filter', '"peakPicking true 2"',
               '--filter', '"precursorRefine"', '-o', MZMLDUMPS]
    process = subprocess.Popen(command, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               creationflags=subprocess_flags)
    (stdout, stderr) = process.communicate()
    if process.returncode != 0 or not os.path.exists(resultpath):
        raise RuntimeError('Error in running msconvert:\n{}'.format(stdout))
    try:
        check_mzml_integrity(resultpath)
    except RuntimeError as e:
        cleanup_files(infile, resultpath)
        self.retry(exc=e)
    cleanup_files(infile)
    return inputstore


def cleanup_files(*files):
    for fpath in files:
        os.remove(fpath)


def copy_infile(rawfile):
    dst = os.path.join(RAWDUMPS, os.path.basename(rawfile))
    print('copying file to local dumpdir')
    shutil.copy(rawfile, dst)
    return dst


def copy_outfile(outfile):
    print('copying result file to outbox')
    dst = os.path.join(OUTBOX, os.path.basename(outfile))
    shutil.copy(outfile, dst)


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
