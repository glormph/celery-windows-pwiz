from celery import Celery

from tasks import config
from tasks.galaxy.util import get_galaxy_instance

TESTING_NO_CLEANUP = True

GALAXY_URL = '130.229.49.225'
APIKEY = '68fd79c85e3bd4887de950f1bf5b679a'  # jorrit on stage


app = Celery('galaxy', backend='amqp')
app.conf.update(
    BROKER_HOST=config.BROKER_URL,
    BROKER_PORT=config.BROKER_PORT,
    CELERY_TASK_SERIALIZER=config.CELERY_TASK_SERIALIZER,
    CELERY_ACCEPT_CONTENT=[config.CELERY_TASK_SERIALIZER],
)


@app.task(queue=config.QUEUE_WORKFLOW)
def cleanup(inputstore):
    #gi = get_galaxy_instance(inputstore)
    pass
    # removes analysis history, mzMLs will be left on disk bc they will be in
    # another history


@app.task(queue=config.QUEUE_GALAXY_WORKFLOW)
def zip_dataset(inputstore):
    """Tar.gz creation of all collection datasets in inputstore which are
    defined as output_dset"""
    # FIXME add MD5 check?
    gi = get_galaxy_instance(inputstore)
    # FIXME check package tool and fix for collections
    ziptool = gi.tools.get_tools(tool_id='package_dataset')[0]
    for dset in inputstore['output_dsets'].values():
        if not dset['src'] == 'hdca':
            continue
        zipdset = gi.tools.run_tool(inputstore['history'], ziptool['id'],
                                    tool_inputs={'method': 'tar', 'input': {
                                        'src': 'hdca', 'id': dset['id']}}
                                    )['outputs'][0]
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
    """Downloads both zipped collections and normal datasets"""
    gi = get_galaxy_instance(inputstore)
    while False not in [x['download_state']
                        for x in inputstore['output_dsets'].values()]:
        for dset in inputstore['output_dsets'].values():
            try:
                download_id = dset['packaged']
            except KeyError:
                download_id = dset['id']
            dset_info = gi.datasets.show_dataset(download_id)
            if dset_info['state'] == 'ok':
                gi.datasets.download_dataset(download_id,
                                             file_path=dset['download_dest'],
                                             use_default_filename=False)
                dset['download_state'] = 'ok'
    return inputstore
