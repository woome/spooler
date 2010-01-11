

"""Views for sigasync - test stuff only right now"""
import logging
import simplejson
from urllib import urlencode

from django.conf import settings
from django.db import transaction
from django.http import HttpRequest
from django.http import HttpResponse
from django.http import HttpResponseServerError
from models import SigasyncTest1
from models import SigasyncTest2

from sigasync_spooler import get_spoolqueue
import pdb
import time

DEFAULT_QUEUE_LABEL="x"

@transaction.commit_manually
def test_1(request):
    if request.method == "POST":
        s = SigasyncTest1(queue_label=request.POST.get("queuelabel", DEFAULT_QUEUE_LABEL))
        s.save()
        transaction.commit()
        return HttpResponse("%d" % s.id)

    return HttpResponse()

def test_2(request, sigasync_test1_id):
    try:
        test_obj1 = SigasyncTest1.objects.get(id=sigasync_test1_id)
        test_obj2 = SigasyncTest2.objects.get(test1=test_obj1)
        return HttpResponse("%d" % test_obj2.test1.id)
    except SigasyncTest2.DoesNotExist:
        return HttpResponseServerError()

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
