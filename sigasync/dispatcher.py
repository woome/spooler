from django.dispatch import dispatcher
from sigasync.sigasync_handler import sigasync_handler as handler

def async_connect(func, **kwargs):
    if not getattr(settings, 'DISABLE_SIGASYNC_SPOOL', False):
        func = handler(func)
    dispatcher.connect(func, **kwargs)

#END

