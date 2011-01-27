
"""Unit tests for sigasync"""

from __future__ import with_statement
import os
import simplejson
import mock
import tempfile
import random
import signal
import logging
import pprint
from shutil import rmtree
from cgi import parse_qsl
from urllib import urlencode
from Queue import Empty
from multiprocessing import Process, Queue
from time import sleep, time
from datetime import datetime

from django.dispatch.dispatcher import Signal
from django.core.cache import cache
from django.db import transaction

from webapp.models import Person
from testsupport.woometestcase import WoomeTestCase
from testsupport.contextmanagers import URLOverride, SettingsOverride

from sigasync.spooler import Spool, SpoolContainer, SpoolManager
from sigasync.sigasync_spooler import SigAsyncContainer, SigAsyncSpool
from sigasync.sigasync_handler import sigasync_handler, send_async
from sigasync.dispatcher import async_connect


class SpoolerTestCase(WoomeTestCase):
    """Test case providing a standard spooler setup."""

    def setUp(self):
        super(SpoolerTestCase, self).setUp()
        self._spool_dir = tempfile.mkdtemp(prefix="testspooler_", dir="/tmp")
        self._override = SettingsOverride(
            DISABLE_SIGASYNC_SPOOL = True,
            SPOOLER_QUEUE_MAPPINGS = {
                'default': 'default',
                'test': 'test',
                'test_http': 'test_http',
                },
            SPOOLER_SPOOLS_ENABLED = ['default', 'test', 'test_http'],
            SPOOLER_DEFAULTS = {'minprocs': 1, 'maxprocs': 1},
            SPOOLER_DIRECTORY = self._spool_dir,
            SPOOLER_DEFAULT = {},
            SPOOLER_TEST = {},
            SPOOLER_VIA_HTTP = ('test_http'),
            )
        self._override.__enter__()

    def tearDown(self):
        self._override.__exit__(None, None, None)
        rmtree(self._spool_dir)
        super(SpoolerTestCase, self).tearDown()

    def get_spool(self, queue='test'):
        return Spool(queue, directory=self._spool_dir)


class SpoolerTest(SpoolerTestCase):
    """Test case for basic spooler functions."""

    def test_standard_submit(self):
        """This tests the submit function of the spooler.

        The test make a file and then submits it to a spooler. The spooler
        processes the file with the default (no-op) processing.

        """
        # Make a file to submit
        (fd, submit_filename) = tempfile.mkstemp(suffix="_spooler_test_file",
                                                 dir="/tmp")
        try:
            with os.fdopen(fd, "w+") as fh:
                fh.write("hello!")

            # Make a spool
            s = Spool("test", directory=self._spool_dir)
            # Submit the pre-existing file to it
            s._submit_file(submit_filename)
            dc = [os.path.join(s._in, d) for d in os.listdir(s._in)]
            assert dc, "the incoming file didn't arrive"
            filename = dc[0]
            with open(filename) as fd:
                content = fd.read()
            assert content == "hello!"

            # Process with the defaults
            s.process()

            # Now assert it's gone to the output
            filename = filename.replace('/in/', '/out/')
            # read the last file.
            try:
                with open(filename) as fd:
                    content = fd.read()
            except IOError:
                assert False, "test1: the processed file didn't arrive"
            assert content == "hello!"
        finally:
            os.unlink(submit_filename)
            try:
                fd.close()
            except Exception:
                pass

    def test_stress(self):
        s = Spool("test", directory=self._spool_dir)
        for i in xrange(1000):
            s._submit_datum("test_submission")

        #start = time()
        s.process()
        #end = time()
        #print end - start


class TestSpoolManager(SpoolManager):
    """Spooler Manager implementing IPC for testing"""
    def __init__(self, *args, **kwargs):
        super(TestSpoolManager, self).__init__(*args, **kwargs)
        self._submitted = Queue()
        self._processed = Queue()
        self._failed = Queue()

    def submitted_entry(self, spool, entry):
        self._submitted.put((spool._name, entry))

    def processed_entry(self, spool, entry):
        self._processed.put((spool._name, entry))

    def failed_entry(self, spool, entry):
        self._failed.put((spool._name, entry))


class MultiprocessingSpoolerTest(SpoolerTestCase):
    def setUp(self):
        super(MultiprocessingSpoolerTest, self).setUp()
        sc = SpoolContainer(directory=self._spool_dir)
        self._container = Process(target=sc.run, args=())
        self._container.start()

    def test_submission(self):
        s = self.get_spool()
        s._submit_datum("test_submission")
        sleep(0.3)
        files = [os.path.join(s._out, f) for f in os.listdir(s._out)]
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
        s = self.get_spool()
        sleep(1)
        for i in xrange(10000):
            s._submit_datum("test_submission")
        sleep(5)

    def tearDown(self):
        os.kill(self._container.pid, signal.SIGINT)
        self._container.join()
        super(MultiprocessingSpoolerTest, self).tearDown()


# Some sigasync handlers for testing:
def pass_handler(sender, instance, **kwargs):
    pass

def print_handler(sender, instance, **kwargs):
    print "Processed!"

def fail_once_handler(sender, instance, **kwargs):
    if cache.get('sigasync_fail') is None:
        cache.set('sigasync_fail', 1, 30 * 60)
        raise Exception("Failing once")
    else:
        cache.delete('sigasync_fail')
        print "Success!"
        start = cache.get('sigasync_test')
        print datetime.now() - start
        cache.set('sigasync_test_finished', 1, 30 * 60)
        return

def fail_handler(sender, instance, **kwargs):
    print "Failing"
    print datetime.now()
    raise Exception("Failing")


class MockSpoolQueue(object):
    def __call__(self, spooler):
        class _MockSpoolQueue(object):
            def _submit_datum(self, data):
                self.data = dict(parse_qsl(data))
            def submit(self, data):
                self._submit_datum(urlencode(data))
            def process(self):
                pass
        sq = _MockSpoolQueue()
        sq.spooler = spooler
        if getattr(self, 'spoolqueue', None):
            self.spoolqueue.append(sq)
        else:
            self.spoolqueue = [sq]
        return sq

class MockPerson(object):
    def __init__(self):
        self.id = random.randint(1, 100000)


class SigAsyncTest(SpoolerTestCase):
    """Tests the spooler running in a separate process"""
    def setUp(self):
        super(self.__class__, self).setUp()
        from django.conf import settings
        try:
            self._disable_sigasync_spool = settings.DISABLE_SIGASYNC_SPOOL
            settings.DISABLE_SIGASYNC_SPOOL = False
        except AttributeError:
            pass

    def tearDown(self):
        from django.conf import settings
        # Restore settings
        try:
            settings.DISABLE_SIGASYNC_SPOOL = self._disable_sigasync_spool
        except AttributeError:
            pass

        super(self.__class__, self).tearDown()

    def test_submit(self):
        self.manager = TestSpoolManager()
        sc = SigAsyncContainer(manager=self.manager)
        self._container = Process(target=sc.run, args=())
        self._container.start()

        async_test1 = Signal()
        async_connect(pass_handler, signal=async_test1, sender=Person)
        p = Person.objects.all()[0]
        async_test1.send(sender=Person, instance=p)
        try:
            self.manager._processed.get(timeout=1)
        except Empty:
            try:
                (s, f) = self.manager._failed.get_nowait()
            except Empty:
                raise AssertionError("Timed out waiting for entry to process")
            else:
                raise AssertionError("Job failed")
        finally:
            os.kill(self._container.pid, signal.SIGINT)
            self._container.join()

    def test_signal_waits_for_commit(self):
        person = MockPerson()
        from sigasync import views, sigasync_spooler, sigasync_handler
        oldview = views.get_spoolqueue
        spoolqueue = MockSpoolQueue()
        views.get_spoolqueue = spoolqueue
        sigasync_spooler.get_spoolqueue = spoolqueue
        sigasync_handler.get_spoolqueue = spoolqueue

        @transaction.commit_on_success
        def helper():
            send_async(pass_handler, 'test', sender=Person, instance=person)
            assert not hasattr(spoolqueue, 'spoolqueue')
            transaction.commit()
            assert len(spoolqueue.spoolqueue) == 1

            send_async(pass_handler, 'test_http',
                       sender=Person, instance=person)
            transaction.commit()
            assert len(spoolqueue.spoolqueue) == 2

            send_async(pass_handler, 'test', sender=Person, instance=person)
            transaction.rollback()
            transaction.commit()
            assert len(spoolqueue.spoolqueue) == 2

            send_async(pass_handler, 'test', sender=Person, instance=person)
        try:
            helper()
            assert len(spoolqueue.spoolqueue) == 3
        finally:
            sigasync_spooler.get_spoolqueue = oldview
            sigasync_handler.get_spoolqueue = oldview
            views.get_spoolqueue = oldview


def check_affinity(sender, table=None, **kwargs):
    assert table is not None
    from django.db import connection
    assert connection.mapper._has_affinity(table)


class MPSigAsyncTest(SpoolerTestCase):
    """Multiprocessing SigAsync tests

    This class manages launching the separate sigasync process and
    communicating with it.

    """
    def setUp(self):
        super(self.__class__, self).setUp()
        from django.conf import settings
        try:
            self._disable_sigasync_spool = settings.DISABLE_SIGASYNC_SPOOL
            settings.DISABLE_SIGASYNC_SPOOL = False
        except AttributeError:
            pass

        self.manager = TestSpoolManager()
        sc = SigAsyncContainer(manager=self.manager)
        self._container = Process(target=sc.run, args=())
        self._container.start()

    def tearDown(self):
        from django.conf import settings
        # Restore settings
        try:
            settings.DISABLE_SIGASYNC_SPOOL = self._disable_sigasync_spool
        except AttributeError:
            pass

        os.kill(self._container.pid, signal.SIGINT)
        self._container.join()

        super(self.__class__, self).tearDown()

    def wait_for_job(self, timeout=1):
        """Waits for a job to finish with a timeout.

        Raises an AssertionError if the job fails or times out.

        """
        try:
            self.manager._processed.get(timeout=1)
        except Empty:
            try:
                (s, f) = self.manager._failed.get_nowait()
            except Empty:
                raise AssertionError("Timed out waiting for entry to process")
            else:
                raise AssertionError("Async process failed, see log")

    def test_affinity_passed_to_spooler(self):
        logger = logging.getLogger(
                        'sigasync.tests.test_affinity_passed_to_spooler')
        from django.db import connection
        if not hasattr(connection, 'mapper'):
            logger.warning('MDB backend required for affinity test')
            return

        # Set regular affinity
        table = 'test1230487'
        connection.mapper._set_affinity(table)
        assert connection.mapper._has_affinity(table)

        # Run a spool job
        send_async(check_affinity, 'test', sender=None, table=table)
        # Wait for it to finish
        # This will raise an error if the job fails
        self.wait_for_job()


def http_test_handler(sender, instance, **kwargs):
    raise Exception('I SHOULD NOT BE CALLED')


class SigasyncHttp(SpoolerTestCase):
    def test_view_params(self):
        data = {}

        def testview(request, spooler):
            data.update({
                'spooler': spooler,
                'data': request.POST.copy(),
            })
            from django.http import HttpResponse
            return HttpResponse('OK')

        person = MockPerson()
        with URLOverride((r'^spooler/(?P<spooler>.+)/$', testview)):
            send_async(pass_handler, 'test_http', sender=Person,
                instance=person, created=True)
            assert data['spooler'] == 'test_http'
            assert data['data']['instance'] == str(person.id)

    def test_submitted_datum(self):
        person = MockPerson()
        from sigasync import views
        oldview = views.get_spoolqueue
        spoolqueue = MockSpoolQueue()
        views.get_spoolqueue = spoolqueue
        try:
            send_async(print_handler, 'test_http', sender=Person,
                instance=person, created=True)
            assert len(spoolqueue.spoolqueue) == 1
            sq = spoolqueue.spoolqueue[-1]
            assert sq.spooler == 'test_http'
            assert sq.data['func_name'] == 'print_handler'
            assert sq.data['func_module'] == 'sigasync.tests'
            assert sq.data['sender'] == 'webapp__Person'
            assert sq.data['instance'] == str(person.id)
            assert sq.data['kwargs'] == '{"created": true}'
        except AssertionError, err:
            try:
                print "Data:"
                pprint.pprint(spoolqueue.spoolqueue[-1].data)
            except:
                pass
            raise err
        finally:
            views.get_spoolqueue = oldview

    def test_http_matches_local(self):
        # Test that local and http submission produce identical results
        from sigasync import views
        oldview = views.get_spoolqueue
        httpqueue = MockSpoolQueue()
        views.get_spoolqueue = httpqueue

        from sigasync import sigasync_handler
        oldhandler = sigasync_handler.get_spoolqueue

        person = MockPerson()
        try:
            send_async(print_handler, 'test_http', sender=Person,
                instance=person, created=True)

            localqueue = MockSpoolQueue()
            sigasync_handler.get_spoolqueue = localqueue
            send_async(print_handler, 'test', sender=Person,
                instance=person, created=True)

            httpdata = httpqueue.spoolqueue[-1].data
            localdata = localqueue.spoolqueue[-1].data
            # remove legitimate differences
            httpdata['spooler'] = 'test'
            httpdata['create_time'] = 0
            localdata['create_time'] = 0
            try:
                assert httpdata == localdata
            except AssertionError:
                pprint.pprint(httpdata)
                pprint.pprint(localdata)
                raise
        finally:
            views.get_spoolqueue = oldview
            sigasync_handler.get_spoolqueue = oldhandler

    def test_list_submission_in_kwargs(self):
        person = MockPerson()
        from sigasync import views
        oldview = views.get_spoolqueue
        spoolqueue = MockSpoolQueue()
        views.get_spoolqueue = spoolqueue

        try:
            send_async(print_handler, 'test_http', Person, instance=person,
                       contacts_id=[1,2,3,4])
            expected_data = simplejson.loads('{"contacts_id": [1, 2, 3, 4]}')
            data = simplejson.loads(spoolqueue.spoolqueue[-1].data['kwargs'])
            assert expected_data['contacts_id'] == data['contacts_id']
        finally:
            views.get_spoolqueue = oldview

    def test_dispatcher_sends_via_http(self):
        from django.conf import settings
        _OLD_HANDLER = settings.SPOOLER_VIA_HTTP
        settings.SPOOLER_VIA_HTTP = ['test']
        test_signal = Signal()
        try:
            async_connect(http_test_handler, spooler='test',
                          signal=test_signal, sender=Person)
            data = {}

            def testview(request, spooler):
                data.update({
                    'spooler': spooler,
                    'data': request.POST.copy(),
                })
                from django.http import HttpResponse
                return HttpResponse('OK')

            person = MockPerson()
            with URLOverride((r'^spooler/(?P<spooler>.+)/$', testview)):
                test_signal.send(instance=person, sender=Person)
                assert data['spooler'] == 'test'
                assert data['data']['instance'] == str(person.id)
        finally:
            settings.SPOOLER_VIA_HTTP = _OLD_HANDLER


mockhandler = mock.Mock()
mockhandler.__module__ = __name__
mockhandler.__name__ = 'mockhandler'

def fakeopen(datum):
    """Returns a Mock object that returns the 'filename' on call to read()."""
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

    def _submit_datum(self, datum):
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
    from django.conf import settings
    return TestSpooler(name, directory=settings.SPOOLER_DIRECTORY)


class SpoolerTimeoutTestCase(SpoolerTestCase):
    def setUp(self):
        super(SpoolerTimeoutTestCase, self).setUp()
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
