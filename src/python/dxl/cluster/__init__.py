from .interactive.web import Request


def submit_task(task):
    return Request.create(task)


def read_task(tid):
    return Request.read(tid)


def read_all():
    return Request.read_all()


def update_task(task):
    return Request.update(task)


def delete_task(tid):
    return Request.delete(tid)
