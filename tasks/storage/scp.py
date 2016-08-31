import os
from paramiko import SSHClient, rsakey
from scp import SCPClient

from celeryapp import app
from tasks import config
from tasks.storage import ftp


@app.task(queue=config.QUEUE_SCP)
def tmp_scp_storage(resultfn, inputstore):
    print('Copying mzML file {} to storage'.format(resultfn))
    scpkey = rsakey.RSAKey.from_private_key_file(config.SCPKEYFILE)
    mzmlfile = os.path.join('/var/data/conversions', resultfn)
    dst = os.path.join(inputstore['storageshare'],
                       inputstore['current_storage_dir'], resultfn)
    ssh = SSHClient()
    ssh.connect(config.STORAGESERVER, username=config.SCP_LOGIN, pkey=scpkey)
    with SCPClient(ssh.get_transport()) as scp:
        scp.put(mzmlfile, dst)
    print('done with {}'.format(resultfn))
    os.remove(mzmlfile)
    return dst
