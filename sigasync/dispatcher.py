from django.conf import settings
from django.dispatch import dispatcher
from sigasync.sigasync_handler import sigasync_handler as handler

def async_connect(func, spooler='default', manager='objects', **kwargs):
    func = handler(func, spooler=spooler, manager=manager)
    dispatcher.connect(func, weak=False, **kwargs)

#END

