
"""Send signals over an asynchronous delivery mechanism"""

import pdb

def sigasync_handler(func):
    print "sigaync_handler called"

    def continuation(sender, instance, created, signal=None, *args, **kwargs):
        # We only allow simple types
        # This is from our original PGQ based transport.

        pdb.set_trace()
        try:
            for k, v in kwargs.iteritems():
                assert type(v) in (str, bool, int)
        except AssertionError:
            raise Exception("kwargs to signal handler '%s' must be of type (str, int, bool) only" % func.__name__)

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
                }.get(created, "0")
            }

        # Submit to the spooler
        import sigasync_spooler
        sigasync_spooler.SPOOLER.submit_datum(urlencode(data))
        
    return continuation

# End
