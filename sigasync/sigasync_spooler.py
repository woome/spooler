import os
import sys
import time
from datetime import datetime, timedelta
try:
    import simplejson
except ImportError, e:
    from django.utils import simplejson
from django.conf import settings
from django.db import models
from django.db import transaction
from django.dispatch.dispatcher import _Anonymous
from spooler import Spool, SpoolContainer, SpoolManager
from spooler import FailError
import logging
from testsupport.contextmanagers import SettingsOverride

def _get_queue_name(name):
    map = settings.SPOOLER_QUEUE_MAPPINGS
    if name in map:
        return map[name]
    elif 'default' in map:
        return map['default']
    else:
        return settings.DEFAULT_SPOOLER_QUEUE_NAME

def get_spoolqueue(name):
    qname = _get_queue_name(name)
    return SigAsyncSpool(qname, directory=settings.SPOOLER_DIRECTORY)


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
        try:
            if settings.DISABLE_SIGASYNC_SPOOL:
                self._processing = self._processing_base
        except AttributeError:
            pass

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


            # Get the func
            func_name = data["func_name"]
            func_module = data["func_module"]
            
            function_object = {}
            exec "import %s ; func_obj=%s.%s" % (func_module, func_module, func_name) in function_object
            del data["func_name"]
            del data["func_module"]

            # Get the instance data
            model = _Anonymous() if data["sender"] == '_Anonymous' else \
                models.get_model(*(data["sender"].split("__")))
            if isinstance(model, _Anonymous):
                instance = data["instance"]
            else:
                try:
                    instance = model.objects.get(id=int(data["instance"]))
                except model.DoesNotExist:
                    raise FailError("%s with id %s not found" %
                                       (model, data['instance']))

            created = {
                "1": True,
                "0": False
                }.get(data["created"], "0")
            data["sender"] = model
            data["instance"] = instance
            data["created"] = created
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

