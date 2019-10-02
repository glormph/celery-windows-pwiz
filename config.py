import os

QUEUE_STORAGE = 'mv_md5_storage'
QUEUE_PWIZ = os.environ.get('PWIZ_QUEUE')
QUEUE_PWIZ_OUT = os.environ.get('PWIZ_OUT_QUEUE')

CERTFILE = os.environ.get('KANTELECERT')
PROTOCOL = 'https://'
KANTELEHOST = '{}{}'.format(PROTOCOL, os.environ.get('KANTELEHOST'))
APIKEY = os.environ.get('APIKEY')
SCP_LOGIN = os.environ.get('SCP_LOGIN')
SSHKEY = os.environ.get('SSHKEY')
KNOWN_HOSTS = os.environ.get('KNOWN_HOSTS')
STORAGESERVER = os.environ.get('STORAGESERVER')

RABBIT_HOST = os.environ.get('RABBITHOST')
RABBIT_USER = 'kantele'
RABBIT_VHOST = 'kantele_vhost'
RABBIT_PASSWORD = os.environ.get('AMQPASS')
BROKER_URL = 'pyamqp://{}:{}@{}/{}'.format(RABBIT_USER, RABBIT_PASSWORD, RABBIT_HOST, RABBIT_VHOST)
BROKER_PORT = 5672
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_BACKEND = 'rpc'

