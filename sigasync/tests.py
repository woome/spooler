
"""Unit tests for sigasync"""

from __future__ import with_statement
import tempfile
import unittest
import pdb
from django.test.client import Client
from sigasync.spooler import *
from multiprocessing import Process
from time import sleep
from os.path import join as pathjoin
import os
import signal
from shutil import rmtree

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
        # XXX This is going to conflict with the testdb setup
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

class SpoolerTest(unittest.TestCase):
    
    def test_standard_submit(self):
        """This tests the submit function of the spooler.

        The test make a file and then submits it to a spooler. The spooler
        processes the file with the default (no-op) processing.

        """
        # Make a file to submit
        (fd, submit_filename) = tempfile.mkstemp(suffix="_spooler_test_file", dir="/tmp")
        fh = None
        try:
            fh = os.fdopen(fd, "w+")
        except Exception, e:
            print e
            assert False, "test1: the submit file %s could not be created?" % submit_filename
        else:
            print >>fh, "hello!"
        finally:
            try:
                fh.close()
            except Exception:
                pass

        # Make a spool
        s=Spool("test")
        # Submit the pre-existing file to it
        s.submit(submit_filename)
        dc = [pathjoin(s._in, direntry) for direntry in os.listdir(s._in)]
        fd = None
        try:
            fd = open(dc[0])
            content = fd.read()
        except Exception:
            assert False, "test1: the incoming file didn't arrive"
        else:
            assert content == "hello!\n" ## we print it, so it has a newline on the end
        finally:
            try:
                fd.close()
            except Exception:
                pass

        # Process with the defaults
        s.process()

        # Now assert it's gone to the output
        dc = [pathjoin(s.get_out_spool(), direntry) for direntry in os.listdir(s.get_out_spool())]
        fd = None
        try:
            fd = open(dc[0])
            content = fd.read()
        except Exception:
            assert False, "test1: the processed file didn't arrive"
        else:
            assert content == "hello!\n" ## we print it, so it has a newline on the end
        finally:
            os.unlink(submit_filename)
            try:
                fd.close()
            except Exception:
                pass

    def test2(self):
        """Test the mounted spooler
        """


        spool_name = "test2"
        try:
            Spool.create(spool_name)
        except SpoolExists:
            pass

        # Make a directory to submit
        spooler_in_dir = "/tmp/test2_spooler_in"
        os.makedirs(spooler_in_dir)

        # Create the pre-existing file which the spooler will process
        submit_filename = pathjoin(spooler_in_dir, "aaaa_spooler_test_file")
        try:
            fd = open(submit_filename, "w")
        except Exception, e:
            assert False, "test1: the submit file %s could not be created?" % submit_filename
        else:
            print >>fd, "hello!"
        finally:
            fd.close()

        # Make a spool
        s=Spool(spool_name, in_spool=spooler_in_dir)
        # Process with the defaults
        s.process()
        # Now assert it's gone to the output
        dc = [pathjoin(s.get_out_spool(), direntry) for direntry in os.listdir(s.get_out_spool())]
        try:
            fd = open(dc[0])
            content = fd.read()
        except Exception:
            assert False, "test1: the processed file didn't arrive"
        else:
            assert content == "hello!\n" ## we print it, so it has a newline on the end
        finally:
            commands.getstatus("rm -rf %s" % spooler_in_dir)
            fd.close()

    def test_stress(self):
        try:
            rmtree('/tmp/test')
        except Exception:
            pass

        s=Spool("test")
        for i in xrange(1000):
            s.submit_datum("test_submission")

        from time import time
        start = time()
        s.process()
        end = time()
        print end - start
        

class MultiprocessingSpoolerTest(unittest.TestCase):
    def setUp(self):
        from django.conf import settings

        # Pick a queue we know exists
        self._queue = settings.SPOOLER_QUEUE_MAPPINGS.values()[0]

        # Set up a SpoolContainer process in a directory we know is empty
        self._spool_dir = tempfile.mkdtemp(prefix="testspooler_", dir="/tmp")
        self._container = Process(target=SpoolContainer,
                                 kwargs={'directory':self._spool_dir})
        self._container.start()
        super(self.__class__, self).setUp()

    def test_submission(self):
        s = Spool(self._queue, directory=self._spool_dir)
        s.submit_datum("test_submission")
        sleep(2)
        files = [pathjoin(s._out, f) for f in os.listdir(s._out)]
        match = False
        for f in files:
            with open(f) as fh:
                content = fh.read()
                if content == "test_submission":
                    match = True
                    break
        assert match, "The processed file was not found."

    def _test_stress(self):
        """Stress test the spooler.

        The test does not actually fail if there is an error, it's only useful
        for manual testing when trying to trigger problems.

        """
        s = Spool(self._queue, directory=self._spool_dir)
        sleep(1)
        for i in xrange(10000):
            s.submit_datum("test_submission")
        sleep(5)

    def tearDown(self):
        os.kill(self._container.pid, signal.SIGINT)
        self._container.join()
        rmtree(self._spool_dir)
        super(self.__class__, self).setUp()

# End
