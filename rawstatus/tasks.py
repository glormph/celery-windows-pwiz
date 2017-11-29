from datasets import config
from celeryapp import app


@app.task(bind=True, queue=config.QUEUE_STORAGE)
def get_md5(self, source_md5, sfid, fnpath, servershare):
    """This will run on remote in other repo so there is no need to be no code
    in here, the task is an empty shell with only the task name"""
    return True


