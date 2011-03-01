import os
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
import sigasync.spooler
from sigasync.spooler import Spool, SpoolContainer, SpoolManager
from sigasync.spooler import SpoolDoesNotExist, FailError

_config = None
def get_config():
    global _config
    if _config is None:
        _config = sigasync.spooler.read_config(settings.SPOOLER_CONFIG)
    return _config

def get_spoolqueue(queue):
    from sigasync.http import HttpSpool
    conf = get_config()
    while queue in conf['mappings']:
        if queue == conf['mappings'][queue]:
            break
        queue = conf['mappings'][queue]
    if queue not in conf['queues']:
        queue = conf['default_queue']
    if queue not in conf['queues']:
        raise SpoolDoesNotExist("No queue named '%s'" % queue)
    qconf = conf['queues'][queue]
    if qconf['http']:
        spool_class = HttpSpool
    else:
        spool_class = SigAsyncSpool
    return spool_class(queue,
                       directory=conf['base_directory'],
                       in_spool=qconf['incoming'],
                       out_spool=qconf['outgoing'],
                       failed_spool=qconf['failure'],
                       shard=qconf['shard'],
                       )

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
    def __init__(self, manager=SigAsyncManager, directory=None,
                 conf=settings.SPOOLER_CONFIG):
        super(SigAsyncContainer, self).__init__(manager, directory, conf)

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

