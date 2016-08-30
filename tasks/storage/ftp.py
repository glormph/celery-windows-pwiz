import os
import ftplib
from tasks import config, dbaccess
from celeryapp import app

from tasks.galaxy import tasks as galaxytasks


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
    ## FIXME REMOVE when scp works
    mzmlfile = os.path.join('/var/data/conversions', mzmlfile)
    ### UNTIL HERE REMOVE
    dst = os.path.split(mzmlfile)[1]
    with open(mzmlfile, 'rb') as fp:
        ftpcon.storbinary('STOR {0}'.format(dst), fp, blocksize=65535)
    print('done with {}'.format(dst))
    ## FIXME REMOVE when scp works
    os.remove(mzmlfile)
    ### UNTIL HERE REMOVE
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
