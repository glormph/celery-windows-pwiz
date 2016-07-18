from celery import Celery
from time import sleep

from tasks import config
from tasks.galaxy.util import get_galaxy_instance

TESTING_NO_CLEANUP = True


app = Celery('galaxy', backend='amqp')
app.conf.update(
    BROKER_HOST=config.BROKER_URL,
    BROKER_PORT=config.BROKER_PORT,
    CELERY_TASK_SERIALIZER=config.CELERY_TASK_SERIALIZER,
    CELERY_ACCEPT_CONTENT=[config.CELERY_TASK_SERIALIZER],
)


@app.task(queue=config.QUEUE_GALAXY_RESULT_TRANSFER)
def download_result(inputstore):
    """Downloads both zipped collections and normal datasets. This is a
    task which can occupy the worker for a long time, since it waits for all
    downloadable datasets to be completed"""
    gi = get_galaxy_instance(inputstore)
    workflow_ok = True
    while workflow_ok and False in [x['download_id'] for x in
                                    inputstore['output_dsets'].values()]:
        workflow_ok = check_output_datasets_wf(gi, inputstore)
        sleep(60)
    if workflow_ok:
        for dset in inputstore['output_dsets'].values():
            gi.datasets.download_dataset(dset['download_id'],
                                         file_path=dset['download_dest'],
                                         use_default_filename=False)
    return inputstore


def check_output_datasets_wf(gi, inputstore):
    """Checks if to-download datasets in workflow are finished, sets their API
    ID as download_id if they are ready, returns workflow_ok status in case
    they are deleted/crashed (False) or not (True)"""
    for dset in inputstore['output_dsets'].values():
        if dset['download_id'] is not False:
            # already checked this dataset
            continue
        try:
            download_id = dset['packaged']
        except KeyError:
            download_id = dset['id']
        dset_info = gi.datasets.show_dataset(download_id)
        if dset_info['state'] == 'ok' and not dset_info['deleted']:
            dset['download_id'] = download_id
        elif dset_info['state'] == 'error' or dset_info['deleted']:
            # Workflow crashed or user intervened, abort downloading
            return False
        return True
