from .exceptions import ServiceNotFound
from . import configs as config_service

services = dict()


def get_default_service(key):
    if key == 'config':       
        return config_service
    elif key == 'database':
        c = get_or_create_service('config').get_config('database')
        if c.use_web_api:
            from .database import api as service
        else:
            from .database.service import Service as service
        return service
    raise ServiceNotFound(key)


def create_service(key, service=None):
    if service is None:
        service = get_default_service(key)
    services[key] = service

# TODO: add support for start/restart/stop service


def get_or_create_service(key):
    if key not in services:
        create_service(key)
    return services[key]
