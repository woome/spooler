import os
import sys
import time
import logging
import threading
from datetime import datetime, timedelta
from collections import deque
from urllib import urlencode
try:
    import simplejson
except ImportError, e:
    from django.utils import simplejson
from django.conf import settings
from django.db import models, connection, transaction
from testsupport.contextmanagers import SettingsOverride
from sigasync.spooler import Spool, SpoolContainer, SpoolManager, FailError

def _get_queue_name(name):
    map = settings.SPOOLER_QUEUE_MAPPINGS
    if name in map:
        return map[name]
    elif 'default' in map:
        return map['default']
    else:
        return settings.DEFAULT_SPOOLER_QUEUE_NAME

def get_spool_conf(queue):
    defaults = getattr(settings, 'SPOOLER_DEFAULTS', {})
    qconf = defaults.copy()
    qconf.update(getattr(settings, 'SPOOLER_%s' % queue.upper(), {}))
    return qconf

def get_spoolqueue(name):
    from sigasync.http import HttpSpool
    qname = _get_queue_name(name)
    if name in settings.SPOOLER_VIA_HTTP:
        qclass = HttpSpool
    else:
        qclass = SigAsyncSpool
    qconf = get_spool_conf(qname)
    return qclass(qname, directory=settings.SPOOLER_DIRECTORY,
                  shard=qconf.get('shard', False))

_local = threading.local()

def _get_transaction_queue():
    """Get the queue of entries waiting for a transaction to finish."""
    if not hasattr(_local, 'queue'):
        _local.queue = deque()
    return _local.queue

def enqueue_datum(datum, spooler):
    """Add datum to the queue for spooler.

    If there is a transaction open it will be held in a secondary queue. It
    will be sent to the spool queue on commit or discarded on rollback.

    """
    logger = logging.getLogger("sigasync.sigasync_spooler")
    if transaction.is_managed():
        queue = _get_transaction_queue()
        queue.append((spooler, datum))
        logger.debug("Holding spooler job for %s" % spooler)
    else:
        get_spoolqueue(spooler).submit(datum)

def handle_commit(sender, **kwargs):
    """Signal handler to flush the transaction queue on a commit."""
    logger = logging.getLogger("sigasync.sigasync_spooler")
    queue = _get_transaction_queue()
    while queue:
        spooler, message = queue.popleft()
        spool = get_spoolqueue(spooler)
        spool.submit(message)
        logger.debug("Running spooler job for %s on commit" % spooler)
        if getattr(settings, 'DISABLE_SIGASYNC_SPOOL', False):
            spool.process()

def handle_rollback(sender, **kwargs):
    """Signal handler to clear the transaction queue on a rollback."""
    logger = logging.getLogger("sigasync.sigasync_spooler")
    queue = _get_transaction_queue()
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Discarding %d jobs due to rollback" % len(queue))
    queue.clear()


class SigAsyncManager(SpoolManager):
    def __init__(self, limit=100):
        super(SigAsyncManager, self).__init__()
        self._processed_count = 0
        self._failed_count = 0
        self._processed_limit = limit
        self._last_reported = datetime.now()
        self._report_interval = timedelta(minutes=5)

    def processed_entry(self, spool, entry):
        self._processed_count += 1
        self._report(spool)

    def failed_entry(self, spool, entry):
        self._failed_count += 1
        self._report(spool)

    def _report(self, spool):
        if datetime.now() - self._last_reported >= self._report_interval:
            logger = logging.getLogger("sigasync.sigasync_spooler.SigAsyncManager")
            logger.info("[queue:%s pid:%s] processed: %s  failed: %s" %
                           (spool._name, os.getpid(),
                            self._processed_count, self._failed_count))

    def finished_processing(self, spool):
        if self._processed_count >= self._processed_limit:
            logging.getLogger("sigasync.sigasync_spooler.SigAsyncManager").info(
                    "Stopping spooler after %s jobs" % self._processed_count)
            self.stop(spool)


class SigAsyncContainer(SpoolContainer):
    def __init__(self, manager=SigAsyncManager, directory=None):
        super(SigAsyncContainer, self).__init__(manager, directory)

    def create_spool(self, queue, queue_settings):
        spool = super(SigAsyncContainer, self).create_spool(
                    queue, queue_settings, spool_class=SigAsyncSpool)
        return spool


class SigAsyncSpool(Spool):
    def __init__(self, name, manager=None, **kwargs):
        if manager is None:
            manager = SigAsyncManager()

        super(SigAsyncSpool, self).__init__(name, manager=manager, **kwargs)
        self.close_transaction_after_execute = False
        if getattr(settings, 'DISABLE_SIGASYNC_SPOOL', False):
            self._processing = self._processing_base

    def submit(self, spec):
        self._submit_datum(urlencode(spec))

    def execute(self, processing_entry):
        logger = logging.getLogger("sigasync.sigasync_spooler.execute")
        try:
            fd = open(processing_entry)
            raw_data = fd.read()

            # Eval the contents of the entry
            import cgi
            pairs = cgi.parse_qsl(raw_data)
            data = dict(pairs)

            create_time = data.pop('create_time', None)
            spooler = data.pop('spooler', None)
            timeout = data.pop('timeout', None)
            if timeout:
                if not create_time:
                    create_time = os.path.ctime(processing_entry)
                if float(create_time) + int(timeout) < time.time():
                    logger.info('job %s returned unprocessed'
                     ' because create time "%s" is older than'
                     ' timeout "%s"' % (processing_entry,
                     create_time, timeout))
                    return

            if 'affinity' in data:
                affinity = simplejson.loads(data['affinity'])
                for k,v in affinity.copy().iteritems():
                    affinity[k] = datetime.strptime(v, "%Y-%m-%d %H:%M:%S")

                if not getattr(settings, 'DISABLE_SIGASYNC_SPOOL', False):
                    try:
                        connection.mapper._affinity = affinity
                    except AttributeError:
                        pass

            # Get the func
            func_name = data["func_name"]
            func_module = data["func_module"]
            
            function_object = {}
            logger.debug("calling: import %s ; func_obj=%s.%s" % (func_module, func_module, func_name))
            exec "import %s ; func_obj=%s.%s" % (func_module, func_module, func_name) in function_object
            del data["func_name"]
            del data["func_module"]

            # Get the instance data
            model = None if data["sender"] == 'None' else \
                models.get_model(*(data["sender"].split("__")))
            if model is None:
                instance = data["instance"]
            else:
                try:
                    instance = model.objects.get(id=int(data["instance"]))
                except model.DoesNotExist:
                    raise FailError("%s with id %s not found" %
                                       (model, data['instance']))

            data["sender"] = model
            data["instance"] = instance
            if 'kwargs' in data:
                kw = simplejson.loads(data['kwargs'])
                if isinstance(kw, dict):
                    for key,val in kw.iteritems():
                        data[key.encode('ascii') if isinstance(key, unicode) else key] = val
                del data['kwargs']

            start = time.time()
            # Call the real handler with the arguments now looking like they did before
            function_object["func_obj"](**data)
            taken = time.time() - start
            logger.info('time taken: %s %s.%s %s %s' %
                           (taken, func_module, func_name,
                            getattr(model, '__name__', None),
                            getattr(instance, 'id', None)))
        finally:
            # Don't catch anything; the process method will handle errors.
            # Just make sure the file is closed.
            if fd:
                try:
                    fd.close()
                except Exception, e:
                    logger.error("failed to read the data in the processing_entry")
            if transaction.is_managed() and self.close_transaction_after_execute:
                transaction.commit()


# End

