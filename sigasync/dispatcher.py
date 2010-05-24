import logging
from sigasync.sigasync_handler import sigasync_handler as handler

def async_connect(func, spooler='default', timeout=None, **kwargs):
    if timeout == 0:
        # no job will be processed, do not do anything
        logging.getLogger('sigasync.dispatcher.async_connect')\
            .warning('timeout set to ZERO!, no jobs will be processed!')
        return
    func = handler(func, spooler=spooler, timeout=timeout)
    try:
        signal = kwargs.pop('signal')
        signal.connect(func, weak=False, **kwargs)
    except KeyError, e:
        logging.getLogger('sigasync.dispatcher.async_connect')\
            .critical('no signal provided, cannot connect!')
        raise
#END

