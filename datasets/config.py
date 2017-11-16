import os

QUEUE_STORAGE = 'mv_md5_storage'
QUEUE_PWIZ1 = 'pwiz1'
QUEUE_PWIZ2 = 'pwiz2'
QUEUE_PWIZ1_OUT = 'proteowiz1_out'
QUEUE_PWIZ2_OUT = 'proteowiz2_out'

CERTFILE = os.environ.get('KANTELECERT')
PROTOCOL = 'https://'
KANTELEHOST = '{}{}'.format(PROTOCOL, os.environ.get('KANTELEHOST'))
APIKEY = os.environ.get('APIKEY')
SCP_LOGIN = os.environ.get('SCP_LOGIN')
PUTTYKEY = os.environ.get('PUTTYKEY')
TMPSHARENAME = 'tmp'
STORAGESHARENAME = 'storage'
STORAGESHARE = os.environ.get('STORAGESHARE')
TMPSHARE = os.environ.get('TMPSHARE')
SHAREMAP = {TMPSHARENAME: TMPSHARE,
            STORAGESHARENAME: STORAGESHARE
            }
STORAGESERVER = os.environ.get('STORAGESERVER')
