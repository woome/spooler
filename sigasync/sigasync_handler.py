"""Send signals over an asynchronous delivery mechanism"""
import time

try:
    import simplejson
except ImportError, e:
    from django.utils import simplejson
from sigasync_spooler import get_spoolqueue

import logging
from django.conf import settings
from django.dispatch.dispatcher import _Anonymous

from sigasync import http

HANDLE_VIA_HTTP = [
    'emailhighpri',
]

def sigasync_handler(func, spooler='default', timeout=None):
    logger = logging.getLogger("sigasync.sigasync_handler")
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("called")

    if spooler in HANDLE_VIA_HTTP:
        def httpsend(instance, sender, *args, **kwargs):
            http.send_handler(spooler, func, instance, sender, *args, **kwargs)
        return httpsend

    def continuation(sender, instance, created=False, signal=None, *args, **kwargs):
        logger = logging.getLogger("sigasync.sigasync_handler.continuation")
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("called")
        
        # We only allow simple types
        # This is from our original PGQ based transport.

        # raises a ValueError if the kwargs cannot be encoded to json
        kwargs_data = simplejson.dumps(kwargs)

        # Make a datum
        from urllib import urlencode
        data = { 
            "func_name": func.__name__,
            "func_module": func.__module__,
            "sender": "%s__%s" % (sender._meta.app_label, sender.__name__) if not isinstance(sender, _Anonymous) else '_Anonymous',
            "instance": instance.id if instance else None,
            "created": { 
                True: "1",
                False: "0"
                }.get(created, "0"),
            "kwargs": kwargs_data,
            "create_time": time.time(),
            "spooler": spooler,
        }
        if timeout:
            data['timeout'] = timeout

        # Submit to the spooler
        spoolqueue = get_spoolqueue(spooler)
        spoolqueue.submit_datum(urlencode(data))
        if getattr(settings, 'DISABLE_SIGASYNC_SPOOL', False):
            spoolqueue.process()
        
    return continuation

# End
