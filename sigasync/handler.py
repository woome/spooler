
"""Django signal handlers for testing sigaync"""

from models import SigasyncTest2

def test_handler(sender, instance, created, **kwargs):
    s = SigasyncTest2(test1=instance)
    s.save()


# End
