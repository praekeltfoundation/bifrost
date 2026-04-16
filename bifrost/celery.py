from celery import Celery

app = Celery("bifrost")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()


@app.task(bind=True)
def debug_task(self):
    return {
        "id": self.request.id,
        "task": self.name,
    }
