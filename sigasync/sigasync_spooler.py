import os
try:
    import simplejson
except ImportError, e:
    from django.utils import simplejson
from django.conf import settings
from django.db import models
from django.db import transaction
from spooler import Spool
from spooler import SpoolExists
from spooler import FailError


class SigAsyncSpool(Spool):
    def __init__(self, name, directory="/tmp", in_spool=None):
        super(SigAsyncSpool, self).__init__(name, directory, in_spool)
        self._failed = os.path.join(self._base, "failed")
        self.close_transaction_after_execute = False
        if not os.path.exists(self._failed):
            os.makedirs(self._failed)

    def _move_to_failed(self, entry):
        os.rename(entry, os.path.join(self._failed, os.path.basename(entry)))

    def execute(self, processing_entry):
        try:
            fd = open(processing_entry)
            raw_data = fd.read()

            # Eval the contents of the entry
            import cgi
            pairs = cgi.parse_qsl(raw_data)
            data = dict(pairs)

            # Get the func
            func_name = data["func_name"]
            func_module = data["func_module"]
            function_object = {}
            exec "import %s ; func_obj=%s.%s" % (func_module, func_module, func_name) in function_object
            del data["func_name"]
            del data["func_module"]

            # Get the instance data
            model = models.get_model(*(data["sender"].split("__")))
            instance = model.objects.get(id=int(data["instance"]))
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

            # Call the real handler with the arguments now looking like they did before
            function_object["func_obj"](**data)
        except FailError, e:
            self._move_to_failed(processing_entry)
            raise
        finally:
            ## FIXME - not sure I need to do this either ...
            ## wil process better handle any problem here?
            if fd:
                try:
                    fd.close()
                except Exception, e:
                    logger.error("failed to read the data in the processing_entry")
            if transaction.is_managed() and self.close_transaction_after_execute:
                transaction.commit()


# Std init
SPOOL_NAME = "sigasync"

# Do we need to manage this with settings? 
# Only if you want to specifically set the spool directory to use
# Maybe if you had radically diffrent signal handling requirements you might wanna do that.
try:
    SigAsyncSpool.create(SPOOL_NAME)
except SpoolExists:
    pass

SPOOLER = SigAsyncSpool(SPOOL_NAME)

# End
