from django.conf import settings
from django.dispatch import dispatcher
from sigasync.sigasync_handler import sigasync_handler as handler

def async_connect(func, spooler='default', **kwargs):
    if not getattr(settings, 'DISABLE_SIGASYNC_SPOOL', False):
        func = handler(func, spooler=spooler)
    dispatcher.connect(func, weak=False, **kwargs)

#END

