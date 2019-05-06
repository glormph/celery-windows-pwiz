from celery import Celery
import config


app = Celery('proteomics-tasks')
app.conf.update(
    broker_host=config.BROKER_URL,
    broker_port=config.BROKER_PORT,
    task_serializer=config.CELERY_TASK_SERIALIZER,
    result_serializer=config.CELERY_TASK_SERIALIZER,
    accept_content=[config.CELERY_TASK_SERIALIZER],
    result_backend=config.CELERY_RESULT_BACKEND,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
)
