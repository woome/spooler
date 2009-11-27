import urllib
from webserviceapp import webservice
from django.dispatch.dispatcher import _Anonymous

SPOOLERHANDLERS = {
    'emailapp.signals.passwordreset_email_handler':     'emailhighpri',
    'emailapp.signals.verification_email_handler':      'emailhighpri',
    'emailapp.signals.email_handler':                   'emailhighpri',
}

def get_spooler(handler):
    return SPOOLERHANDLERS.get(handler, 'default')

def send(instance, sender, handler, **kwargs):
    spooler = get_spooler(handler)
    data = {
        'instance': instance.id,
        'sender': '%s__%s' % (sender._meta.app_label, sender.__name__) if not isinstance(sender, _Anonymous) else '_Anonymous',
        'handler': handler,
    }

    if kwargs:
        data.update(kwargs)
    
    return webservice.post('/spooler/%s/' % spooler, data)

