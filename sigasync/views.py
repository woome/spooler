import logging
import time
from urllib import urlencode

from django.conf import settings
from django.http import HttpResponse

from sigasync.sigasync_spooler import get_spoolqueue

def spooler_http_gateway(request, spooler):
    logger = logging.getLogger("sigasync.views.spooler_http_gateway")
    data = request.POST.copy()
    cleaned_data = {}
    
    for k in data:
        cleaned_data[k] = data.getlist(k) if len(data.getlist(k)) > 1 else data.get(k)
    
    spoolqueue = get_spoolqueue(spooler)
    # This can't use submit() because that does the http post
    spoolqueue._submit_datum(urlencode(cleaned_data))
    if getattr(settings, 'DISABLE_SIGASYNC_SPOOL', False):
        spoolqueue.process()
    return HttpResponse('OK')
        
# End
