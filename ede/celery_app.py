from celery import Task, Celery

celery_app = Celery(__name__)
celery_app.config_from_object('ede.celery_settings')

