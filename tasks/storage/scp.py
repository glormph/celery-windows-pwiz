import os
from paramiko import SSHClient, rsakey
from scp import SCPClient
from hashlib import md5

from celeryapp import app
from tasks import config

# import ftp since it is in a chain with scp task
from tasks.storage import ftp


@app.task(queue=config.QUEUE_SCP, bind=True)
def tmp_scp_storage(self, inputstore):
    mzmlfile = os.path.join('/var/data/conversions', inputstore['mzml'])
    hashmd5 = md5()
    with open(mzmlfile, 'rb') as fp:
        for chunk in iter(lambda: fp.read(4096), b''):
            hashmd5.update(chunk)
    mzml_md5 = hashmd5.hexdigest()
    print('Copying mzML file {} with md5 {} to storage'.format(
        inputstore['mzml'], mzml_md5))
    scpkey = rsakey.RSAKey.from_private_key_file(config.SCPKEYFILE)
    dst = os.path.join(inputstore['storage_localpath'],
                       inputstore['current_storage_dir'], inputstore['mzml'])
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(config.STORAGESERVER, username=config.SCP_LOGIN, pkey=scpkey)
    try:
        with SCPClient(ssh.get_transport()) as scp:
            scp.put(mzmlfile, dst)
    except:
        # FIXME probably better to not retry? put in dead letter queue?
        # usually when this task has probelsm it is usually related to network
        self.retry(countdown=60)
    # FIXME check md5 and rerun cannot be done here because this task runs not
    # on storage server but on remote VM...
    try:
        stdin, stdout, stderr = ssh.exec_command('md5sum {}'.format(dst))
        dst_md5 = stdout.read().split()[0]
    except:
        # FIXME probably better to not retry? put in dead letter queue?
        # usually when this task has probelsm it is usually related to network
        self.retry(countdown=60)
    ssh.close()
    if not dst_md5 == mzml_md5:
        # FIXME drop in dead letter, do not retry? if the error was network,
        # the previous block will have errored already so retry here should be
        # feasible
        self.retry(countdown=60)
        return
    inputstore['mzml_md5'] = mzml_md5
    inputstore['mzml_path'] = dst
    print('done with {}'.format(inputstore['mzml']))
    # how to fail database connection? should they be in different tasks?
    # or should db be updated in this task? multiple points of failure, make
    # sure retry and other failure modes exist!
    #dbaccess.store_mzmlfile(resultfn['dbid'], mzml_md5)
    os.remove(mzmlfile)
    return inputstore
