
from django.conf import settings
from django.db import models
from spooler import Spool
from spooler import SpoolExists

import pdb

class SigAsyncSpool(Spool):
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
            import pdb
            pdb.set_trace()
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

            # Call the real handler with the arguments now looking like they did before
            function_object["func_obj"](**data)
        finally:
            ## FIXME - not sure I need to do this either ...
            ## wil process better handle any problem here?
            if fd:
                try:
                    fd.close()
                except Exception, e:
                    logger.error("failed to read the data in the processing_entry")


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
