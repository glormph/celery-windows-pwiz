import sys
import os
import subprocess
import shutil

from celeryapp import app
from tasks import config

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


def copy_infile(rawfile):
    dst = os.path.join(RAWDUMPS, os.path.basename(rawfile))
    print('copying file to local dumpdir')
    shutil.copy(rawfile, dst)
    return dst


def copy_outfile(outfile):
    print('copying result file to outbox')
    dst = os.path.join(OUTBOX, os.path.basename(outfile))
    shutil.copy(outfile, dst)
