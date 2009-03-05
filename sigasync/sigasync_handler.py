"""Send signals over an asynchronous delivery mechanism"""
try:
    import simplejson
except ImportError, e:
    from django.utils import simplejson
from sigasync_spooler import get_spoolqueue

def sigasync_handler(func, spooler='default'):
    print "sigaync_handler called"

    def continuation(sender, instance, created=False, signal=None, *args, **kwargs):
        # We only allow simple types
        # This is from our original PGQ based transport.

        # raises a ValueError if the kwargs cannot be encoded to json
        kwargs_data = simplejson.dumps(kwargs)

        # Make a datum
        from urllib import urlencode
        data = { 
            "func_name": func.__name__,
            "func_module": func.__module__,
            "sender": "%s__%s" % (sender._meta.app_label, sender.__name__),
            "instance": instance.id,
            "created": { 
                True: "1",
                False: "0"
                }.get(created, "0"),
            "kwargs": kwargs_data,
        }

        # Submit to the spooler
        spoolqueue = get_spoolqueue(spooler)
        spoolqueue.submit_datum(urlencode(data))
        
    return continuation

# End
