import logging

from django.conf import settings
from django.dispatch import dispatcher
from sigasync.sigasync_handler import sigasync_handler as handler

def async_connect(func, spooler='default', timeout=None, **kwargs):
    if timeout == 0:
        # no job will be processed, do not do anything
        logging.getLogger('sigasync.dispatcher.async_connect')\
            .warning('timeout set to ZERO!, no jobs will be processed!')
        return
    func = handler(func, spooler=spooler, timeout=timeout)
    dispatcher.connect(func, weak=False, **kwargs)

#END

