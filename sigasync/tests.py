
"""Unit tests for sigasync"""

import unittest
import pdb
from django.test.client import Client


class SigasyncQueue(unittest.TestCase):
    """Basic tests for the SigAsync queue

    These tests illustrate the flexibility of the queue... you can
    have the asynchronous delivery AND still have synchronous tests.
    """

    def setUp(self):
        from django.conf import settings
        settings.DISABLE_SIGASYNC_SPOOL = False
        self.client = Client()

    def tearDown(self):
        from django.conf import settings
        settings._import_settings()

    def test_queue(self):
        """Make two related objects, 1 of them via the queued_handler"""

        # Post to a view to cause the objects to be created
        response1 = self.client.post("/sigasync/v1/")
        self.assertEquals(response1.status_code, 200)
        test_obj_1_id = int(response1.content)
        
        # Now retrieve the data about the 2nd object which is going to be created by the signal
        response2 = self.client.get("/sigasync/v2/%d/" % test_obj_1_id)
        self.assertEquals(response2.status_code, 500)
        
        # Now get the queue to process
        from sigasync.sigasync_spooler import get_spoolqueue
        spooler = get_spoolqueue('default')
        spooler.process()
        
        # Now try the retrievel again
        response3 = self.client.get("/sigasync/v2/%d/" % test_obj_1_id)
        self.assertEquals(response3.status_code, 200)
        self.assertEquals(int(response3.content), test_obj_1_id)

# End
