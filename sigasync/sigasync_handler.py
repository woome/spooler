"""Send signals over an asynchronous delivery mechanism"""
import time
import logging
from functools import partial

from django.utils import simplejson
from django.core.serializers.json import DjangoJSONEncoder

from django.db import connection
from django.conf import settings

from sigasync.sigasync_spooler import get_spoolqueue, enqueue_datum
from sigasync import http

def sigasync_handler(func, spooler='default', timeout=None):
    return partial(send_async, func, spooler, timeout=timeout)

def send_async(func, spooler, sender, instance=None, timeout=None, signal=None, **kwargs):
    
    # persists modified_attrs if available for async signal handler
    if getattr(instance, '_modified_attrs_were', None):
        kwargs['_modified_attrs'] = instance._modified_attrs_were
    # raises a ValueError if the kwargs cannot be encoded to json
    kwargs_data = simplejson.dumps(kwargs, cls=DjangoJSONEncoder)

    # Make a datum
    data = {
        "func_name": func.__name__,
        "func_module": func.__module__,
        "sender": "%s__%s" % (sender._meta.app_label, sender.__name__) \
            if sender is not None else 'None',
        "instance": instance.id if instance else None,
        "kwargs": kwargs_data,
        "create_time": time.time(),
        "spooler": spooler,
    }
    if timeout:
        data['timeout'] = timeout

    if hasattr(connection, 'mapper'):
        data['affinity'] = simplejson.dumps(connection.mapper._affinity, cls=DjangoJSONEncoder)

    if getattr(settings, 'DISABLE_SIGASYNC_SPOOL', False):
        spoolqueue = get_spoolqueue(spooler)
        spoolqueue.submit(data)
        spoolqueue.process()
    else:
        enqueue_datum(data, spooler)

# End
