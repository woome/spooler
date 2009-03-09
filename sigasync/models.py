
"""models for sigasync - test stuff"""

from django.db import models
from django.db.models.signals import post_save



class SigasyncTest1(models.Model):
    queue_label = models.CharField(max_length=40)

class SigasyncTest2(models.Model):
    test1 = models.ForeignKey(SigasyncTest1, raw_id_admin=True)


# Setup the connections to the signal handlers
from sigasync import handler
from sigasync import sigasync_handler
try:
    #post_save.connect(queued_handler.sigasync_handler(handler.test_handler), 
    #                  sender=SigasyncTest1)
    raise Exception("dummy")
except:
    # pre-django 1.0
    from sigasync.dispatcher import async_connect
    async_connect(handler.test_handler,
                       manager='objects',
                       signal=post_save, 
                       sender=SigasyncTest1)


# End
