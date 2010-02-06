
"""Unit tests for sigasync"""

from __future__ import with_statement
from cgi import parse_qs
import simplejson
import mock
import tempfile
import unittest
import pdb
from django.test.client import Client
from sigasync.spooler import *
from sigasync.sigasync_spooler import SigAsyncContainer
from sigasync.sigasync_spooler import SigAsyncSpool
from sigasync.sigasync_handler import sigasync_handler
from multiprocessing import Process
from time import sleep, time
from os.path import join as pathjoin
import os
import signal
from shutil import rmtree
from datetime import datetime
from sigasync.dispatcher import async_connect
from django.dispatch.dispatcher import Signal
from django.core.cache import cache
from webapp.models import Person
from testsupport.woometestcase import WoomeTestCase
from testsupport.contextmanagers import URLOverride
import sigasync.http


class SigasyncQueue(unittest.TestCase):
    """Basic tests for the SigAsync queue

    These tests illustrate the flexibility of the queue... you can
    have the asynchronous delivery AND still have synchronous tests.
    """

    def setUp(self):
        from django.conf import settings
        try:
            self._disable_sigasync_spool_saved = settings.DISABLE_SIGASYNC_SPOOL
            settings.DISABLE_SIGASYNC_SPOOL = False
        except AttributeError:
            pass
        self.client = Client()

    def tearDown(self):
        from django.conf import settings
        try:
            settings.DISABLE_SIGASYNC_SPOOL = self._disable_sigasync_spool_saved
        except AttributeError:
            pass

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
        filename = None
        try:
            filename = dc[0]
            fd = open(filename)
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
        # dc = [pathjoin(s.get_out_spool(), direntry) for direntry in os.listdir(s.get_out_spool())]
        fd = None
        try:
            filename = filename.replace('/in/', '/out/')
            # read the last file.
            fd = open(filename)
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
        sc = SpoolContainer(directory=self._spool_dir)
        self._container = Process(target=sc.run, args=())
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


# Some sigasync handlers for testing:
def pass_handler(sender, instance, **kwargs):
    pass

def print_handler(sender, instance, **kwargs):
    start = cache.get('sigasync_test')
    print "Processed! in %s" % (datetime.now() - start)
    cache.set('sigasync_test_finished', 1, 30*60)

def fail_once_handler(sender, instance, **kwargs):
    if cache.get('sigasync_fail') is None:
        cache.set('sigasync_fail', 1, 30*60)
        raise Exception("Failing once")
    else:
        cache.delete('sigasync_fail')
        print "Success!"
        start = cache.get('sigasync_test')
        print datetime.now() - start
        cache.set('sigasync_test_finished', 1, 30*60)
        return

def fail_handler(sender, instance, **kwargs):
    print "Failing"
    print datetime.now()
    raise Exception("Failing")

class SigAsyncTest(unittest.TestCase):
    def setUp(self):
        from django.conf import settings
        # Cache settings that we're going to change
        try:
            self._disable_sigasync_spool_saved = settings.DISABLE_SIGASYNC_SPOOL
            settings.DISABLE_SIGASYNC_SPOOL = False
        except AttributeError:
            pass

        self._spooler_directory = settings.SPOOLER_DIRECTORY

        # Pick a queue we know exists
        self._queue = settings.SPOOLER_QUEUE_MAPPINGS.values()[0]

        # Set up a SpoolContainer process in a directory we know is empty
        self._spool_dir = tempfile.mkdtemp(prefix="testspooler_", dir="/tmp")
        settings.SPOOLER_DIRECTORY = self._spool_dir
        sc = SigAsyncContainer(directory=self._spool_dir)
        self._container = Process(target=sc.run, args=())
        self._container.start()
        super(self.__class__, self).setUp()

    def _test_submit(self):
        async_test1 = Signal()
        async_connect(print_handler, signal=async_test1, sender=Person)
        start = datetime.now()
        cache.set('sigasync_test', start, 30*60)
        p = Person.objects.all()[0]
        async_test1.send(sender=Person, instance=p)
        while cache.get('sigasync_test_finished') is None:
            sleep(1)
        cache.delete('sigasync_test')
        cache.delete('sigasync_test_finished')

    def tearDown(self):
        from django.conf import settings
        # Restore settings
        try:
            settings.DISABLE_SIGASYNC_SPOOL = self._disable_sigasync_spool_saved
        except AttributeError:
            pass
        settings.SPOOLER_DIRECTORY = self._spooler_directory

        os.kill(self._container.pid, signal.SIGINT)
        self._container.join()
        rmtree(self._spool_dir)
        super(self.__class__, self).setUp()

class MockSpoolQueue(object):
    def __call__(self, spooler):
        class _MockSpoolQueue(object):
            def submit_datum(self, data):
                self.data = parse_qs(data)
            def process(self):
                pass
        sq = _MockSpoolQueue()
        sq.spooler = spooler
        if getattr(self, 'spoolqueue', None):
            self.spoolqueue.append(sq)
        else:
            self.spoolqueue = [sq]
        return sq


class SigasyncHttp(WoomeTestCase):
    def test_view_params(self):
        data = {}
        def testview(request, spooler):
            data.update({
                'spooler': spooler,
                'data': request.POST.copy(),
            })
            from django.http import HttpResponse
            return HttpResponse('OK')
        person = self.reg_and_get_person('ht')
        with URLOverride((r'^spooler/(?P<spooler>.+)/$', testview)):
            sigasync.http.send(instance=person, sender=Person,
                handler='emailapp.signals.passwordreset_email_handler',
                created=True)
            assert data['spooler'] == 'emailhighpri'
            assert data['data']['instance'] == str(person.id)

    def test_submitted_datum(self):
        person = self.reg_and_get_person('ht')
        from sigasync import views
        oldview = views.get_spoolqueue
        spoolqueue = MockSpoolQueue()
        views.get_spoolqueue = spoolqueue
        try:
            sigasync.http.send(instance=person, sender=Person,
                handler='emailapp.signals.passwordreset_email_handler',
                created=True)
            assert spoolqueue.spoolqueue[-1].spooler == 'emailhighpri'
            assert spoolqueue.spoolqueue[-1].data['func_name'] == ['passwordreset_email_handler']
            assert spoolqueue.spoolqueue[-1].data['func_module'] == ['emailapp.signals']
            assert spoolqueue.spoolqueue[-1].data['sender'] == ['webapp__Person']
            assert spoolqueue.spoolqueue[-1].data['instance'] == [str(person.id)]
            assert spoolqueue.spoolqueue[-1].data['created'] == ['1']
            assert len(spoolqueue.spoolqueue) == 1
        finally:
            views.get_spoolqueue = oldview
    
    
    def test_list_submission_in_kwargs(self):
        person = Person.not_banned.latest('id')
        from sigasync import views
        oldview = views.get_spoolqueue
        spoolqueue = MockSpoolQueue()
        views.get_spoolqueue = spoolqueue
        
        try:
            sigasync.http.send(instance=person, sender=Person,
                handler='emailapp.signals.contacts_siteinvites_handler',
                created=True, contacts_id=[1,2,3,4])
            expected_contacts_id = simplejson.loads('{"contacts_id": ["1", "2", "3", "4"]}')['contacts_id']
            assert simplejson.loads(spoolqueue.spoolqueue[-1].data['kwargs'][0])['contacts_id'] == expected_contacts_id
        finally:
            views.get_spoolqueue = oldview

    def test_dispatcher_sends_via_http(self):
        from django.db.models import signals
        from sigasync import sigasync_handler
        from django.conf import settings
        _OLD_HANDLER = settings.SPOOLER_VIA_HTTP
        settings.SPOOLER_VIA_HTTP = ['test']
        try:
            async_connect(http_test_handler, spooler='test', signal=test_signal, sender=Person)
            data = {}
            def testview(request, spooler):
                data.update({
                    'spooler': spooler,
                    'data': request.POST.copy(),
                })
                from django.http import HttpResponse
                return HttpResponse('OK')
            person = self.reg_and_get_person('ht')
            with URLOverride((r'^spooler/(?P<spooler>.+)/$', testview)):
                test_signal.send(instance=person, sender=Person)
                assert data['spooler'] == 'test'
                assert data['data']['instance'] == str(person.id)
        finally:
            settings.SPOOLER_VIA_HTTP = _OLD_HANDLER


test_signal = Signal()
def http_test_handler(sender, instance, **kwargs):
    raise Exception('I SHOULD NOT BE CALLED')



mockhandler = mock.Mock()
mockhandler.__module__ = __name__
mockhandler.__name__ = 'mockhandler'

def fakeopen(datum):
    """Returns a Mock object that returns the 'filename'i on call to read()."""
    filemock = mock.Mock()
    filemock.read.return_value = datum
    return filemock

class TestSpooler(SigAsyncSpool):
    """A test Spool subclass that doesn't write to disk.
    Instead it stores the datums, and processes them on process.
    Patches open to read the datum rather than a file.

    """
    def __init__(self, *args, **kwargs):
        super(TestSpooler, self).__init__(*args, **kwargs)
        self._submitted_datums = []
        self._processed_datums = []

    def submit_datum(self, datum):
        self._submitted_datums.append(datum)

    #@mock.patch('sigasync.sigasync_spooler.open', fakeopen)
    def process(self, function=None):
        import sigasync.sigasync_spooler
        self._processed_datums = []
        try:
            sigasync.sigasync_spooler.open = fakeopen
            while True:
                if not len(self._submitted_datums):
                    break
                entry = self._submitted_datums.pop(0)
                self.execute(entry)
                self._processed_datums.append(entry)
        finally:
            del sigasync.sigasync_spooler.open

def mock_getspoolqueue(name):
    """Returns a TestSpooler instance when called."""
    return TestSpooler(name)

class SpoolerTimeoutTestCase(unittest.TestCase):
    def setUp(self):
        mockhandler.reset_mock()

    # patch get_spoolqueue to return our test spooler
    @mock.patch('sigasync.sigasync_handler.get_spoolqueue', mock_getspoolqueue)
    def test_existing_behaviour(self):
        # create our signal handler without a timeout
        handler = sigasync_handler(mockhandler, spooler='test')
        # send it a message
        handler(sender=None, instance=None)
        # check our handler was called as expected
        assert len(mockhandler.call_args_list) == 1

    # patch get_spoolqueue to return our test spooler
    @mock.patch('sigasync.sigasync_handler.get_spoolqueue', mock_getspoolqueue)
    def test_normal_submission(self):
        # create our signal handler with a timeout
        handler = sigasync_handler(mockhandler, spooler='test', timeout=10)
        # send it a message
        handler(sender=None, instance=None)
        # check our handler was called as expected
        assert len(mockhandler.call_args_list) == 1

    @mock.patch('sigasync.sigasync_handler.get_spoolqueue', mock_getspoolqueue)
    def test_expired_submission(self):
        # create our signal handler with a timeout
        handler = sigasync_handler(mockhandler, spooler='test', timeout=10)
        with mock.patch('sigasync.sigasync_spooler.time') as mtime:
            # advance time by 11 seconds. this should discard job
            mtime.time.side_effect = lambda: time() + 11
            # send job to handler
            handler(sender=None, instance=None)
            # check our patched time was called for sanity
            assert mtime.time.called
            # our handler should not have been called
            assert not mockhandler.called, mockhandler.call_args_list


# End