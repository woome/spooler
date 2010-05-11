import logging
import simplejson
import time
from urllib import urlencode

from django.conf import settings
from django.http import HttpResponse

from sigasync_spooler import get_spoolqueue

def spooler_http_gateway(request, spooler):
    logger = logging.getLogger("sigasync.views.spooler_http_gateway")
    kwargs = request.POST.copy()
    sender = kwargs.pop('sender')[0]
    instance = kwargs.pop('instance')[0]
    created = kwargs.pop('created', [None])[0]
    timeout = kwargs.pop('timeout', [None])[0]
    func_module, _, func_name = kwargs.pop('handler')[0].rpartition('.')
    data = { 
        "func_name": func_name,
        "func_module": func_module,
        "sender": sender,
        "instance": instance if instance else None,
        "created": { 
            'True': "1",
            'False': "0"
        }.get(created, "0"),
        "create_time": time.time(),
        "spooler": spooler
    }

    if timeout:
        data['timeout'] = timeout
    
    cleaned_kwargs = {}
    
    for k in kwargs:
        cleaned_kwargs[k] = kwargs.getlist(k) if len(kwargs.getlist(k)) > 1 else kwargs.get(k)
    
    data.update({
        'kwargs': simplejson.dumps(cleaned_kwargs)
    })
    
    spoolqueue = get_spoolqueue(spooler)
    spoolqueue.submit_datum(urlencode(data))
    if getattr(settings, 'DISABLE_SIGASYNC_SPOOL', False):
        spoolqueue.process()
    return HttpResponse('OK')
        
# End
