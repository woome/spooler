import urllib
from webserviceapp import webservice

SPOOLERHANDLERS = {
    'emailapp.newmailsignals.siteinvite_email_handler': 'emailhighpri_http',
}

def get_spooler(handler):
    return SPOOLERHANDLERS.get(handler, 'default')

def send(instance, sender, handler, **kwargs):
    spooler = get_spooler(handler)
    data = {
        'instance': instance.id,
        'sender': '%s__%s' % (sender._meta.app_label, sender.__name__) if not sender is None else 'None',
        'handler': handler,
    }

    if kwargs:
        data.update(kwargs)
    
    return webservice.post('/spooler/%s/' % spooler, data)

def send_handler(spooler, handler, instance, sender, **kwargs):
    data = {
        'instance': instance.id,
        'sender': '%s__%s' % (sender._meta.app_label, sender.__name__) if not sender is None else 'None',
        'handler': '%s.%s' % (handler.__module__, handler.__name__),
    }

    if kwargs:
        data.update(kwargs)
    
    return webservice.post('/spooler/%s/' % spooler, data)
