from django.dispatch import dispatcher
from sigasync.sigasync_handler import sigasync_handler as handler

def async_connect(func, **kwargs):
    dispatcher.connect(handler(func), **kwargs)

#END

