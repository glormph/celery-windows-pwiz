import sys
import os
import subprocess
import shutil
from hashlib import md5


from celeryapp import app
from tasks import config


PROTEOWIZ_LOC = ('C:\Program Files\ProteoWizard\ProteoWizard '
                 '3.0.11336\msconvert.exe')
PSCP_LOC = ('C:\Program Files\PuTTY\pscp.exe')
RAWDUMPS = 'C:\\rawdump'
MZMLDUMPS = 'C:\\mzmldump'
OUTBOX = 'X:'


@app.task(queue=config.QUEUE_PWIZ1IO, bind=True)
def tmp_scp_storage(self, inputstore):
    mzmlfile = os.path.join(MZMLDUMPS, inputstore['mzml'])
    print('Got copy-to-storage command, calculating MD5 for file '
          '{}'.format(inputstore['mzml']))
    hashmd5 = md5()
    with open(mzmlfile, 'rb') as fp:
        for chunk in iter(lambda: fp.read(4096), b''):
            hashmd5.update(chunk)
    mzml_md5 = hashmd5.hexdigest()
    print('Copying mzML file {} with md5 {} to storage'.format(
        inputstore['mzml'], mzml_md5))
    dstfolder = os.path.join(config.STORAGESHARE,
                             inputstore['current_storage_dir']).replace('\\', '/')
    dst = '{}@{}:{}'.format(config.SCP_LOGIN, config.STORAGESERVER, dstfolder)
    try:
        subprocess.check_call([PSCP_LOC, '-i', config.PUTTYKEY, mzmlfile, dst])
    except:
        # FIXME probably better to not retry? put in dead letter queue?
        # usually when this task has probelsm it is usually related to network
        # or corrupt file, both of which are not nice to retry
        self.retry(countdown=60)
    print('Copied file, calculating MD5')
    arrived_file = os.path.join(inputstore['winshare'],
                                inputstore['current_storage_dir'],
                                os.path.basename(mzmlfile))
    hashmd5 = md5()
    try:
        with open(arrived_file, 'rb') as fp:
            for chunk in iter(lambda: fp.read(4096), b''):
                hashmd5.update(chunk)
        dst_md5 = hashmd5.hexdigest()
    except:
        # FIXME probably better to not retry? put in dead letter queue?
        # usually when this task has probelsm it is usually related to network
        print('MD5 calculation failed, check file location at {}'.format(arrived_file))
        self.retry(countdown=60)
    if not dst_md5 == mzml_md5:
        print('Destination MD5 {} is not same as source MD5 {}'.format(dst_md5, mzml_md5))
        self.retry(countdown=60)
        return
    inputstore['mzml_md5'] = mzml_md5
    inputstore['mzml_path'] = dst
    print('done with {}'.format(inputstore['mzml']))
    os.remove(mzmlfile)
    return inputstore


@app.task(bind=True, queue=config.QUEUE_PWIZ1)
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
    print('Received conversion command for file {0}'.format(inputstore['raw']))
    infile = os.path.join(RAWDUMPS, inputstore['raw'])
    outfile = os.path.splitext(inputstore['raw'])[0] + '.mzML'
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


@app.task(queue=config.QUEUE_PWIZ1IO, bind=True)
def copy_infile(self, inputstore):
    remote_file = os.path.join(inputstore['winshare'],
                               inputstore['current_storage_dir'],
                               inputstore['raw'])
    dst = os.path.join(RAWDUMPS, inputstore['raw'])
    print('copying file to local dumpdir')
    try:
        shutil.copy(remote_file, dst)
    except Exception as e:
        try:
            cleanup_files(dst)
        # windows specific error
        except FileNotFoundError:
            pass
        print('{} -- WARNING, could not copy input {} to local '
              'disk'.format(e, dst))
    print('Done copying file to local dumpdir')
    return inputstore


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
