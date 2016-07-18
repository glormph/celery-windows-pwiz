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


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW)
def cleanup(inputstore):
    #gi = get_galaxy_instance(inputstore)
    pass
    # removes analysis history, mzMLs will be left on disk bc they will be in
    # another history


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW, bind=True)
def zip_dataset(self, inputstore):
    """Tar.gz creation of all collection datasets in inputstore which are
    defined as output_dset"""
    # FIXME add MD5 check?
    gi = get_galaxy_instance(inputstore)
    # FIXME check package tool and fix for collections
    try:
        ziptool = gi.tools.get_tools(tool_id='package_dataset')[0]
    except:
        self.retry(countdown=60)
    for dset in inputstore['output_dsets'].values():
        if not dset['src'] == 'hdca':
            continue
        try:
            zipdset = gi.tools.run_tool(inputstore['history'], ziptool['id'],
                                        tool_inputs={'method': 'tar', 'input':
                                                     {'src': 'hdca',
                                                      'id': dset['id']}}
                                        )['outputs'][0]
        except:
            self.retry(countdown=60)
        dset['packaged'] = zipdset['id']
    return inputstore


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW)
def zip_dataset_oldstyle(inputstore):
    """Tar.gz creation of all collection datasets in inputstore which are
    defined as output_dset"""
    # FIXME will only work  on oldstyle prod, deprecate when updated
    gi = get_galaxy_instance(inputstore)
    ziptool = gi.tools.get_tools(tool_id='package_dataset')[0]
    for dset in inputstore['output_dsets'].values():
        zipdset = gi.tools.run_tool(inputstore['history'], ziptool['id'],
                                    tool_inputs={'method': 'tar', 'input': {
                                        'src': 'hda', 'id': dset['id']}}
                                    )['outputs'][0]
        dset['packaged'] = zipdset['id']
    return inputstore


@app.task(queue=config.QUEUE_GALAXY_RESULT_TRANSFER)
def download_result(inputstore):
    """Downloads both zipped collections and normal datasets. This is a
    task which can occupy the worker for a long time, since it waits for all
    downloadable datasets to be completed"""
    gi = get_galaxy_instance(inputstore)
    workflow_ok = True
    while workflow_ok and False in [x['download_id'] for x in
                                    inputstore['output_dsets'].values()]:
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
                workflow_ok = False
        sleep(60)
    if workflow_ok:
        for dset in inputstore['output_dsets'].values():
            gi.datasets.download_dataset(dset['download_id'],
                                         file_path=dset['download_dest'],
                                         use_default_filename=False)
    return inputstore
