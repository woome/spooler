
"""Unit tests for sigasync"""
from __future__ import with_statement

from cgi import parse_qs
import unittest
import pdb, simplejson
from django.test.client import Client
from testsupport.woometestcase import WoomeTestCase
from testsupport.contextmanagers import URLOverride
from webapp.models import Person

import sigasync.http
from sigasync.dispatcher import async_connect

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


class MockSpoolQueue(object):
    def __call__(self, spooler):
        class _MockSpoolQueue(object):
            def submit_datum(self, data):
                self.data = parse_qs(data)
            def process(self):
                pass
        sq = _MockSpoolQueue()
        sq.spooler = spooler
        self.spoolqueue = sq
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
            assert spoolqueue.spoolqueue.spooler == 'emailhighpri'
            assert spoolqueue.spoolqueue.data['func_name'] == ['passwordreset_email_handler']
            assert spoolqueue.spoolqueue.data['func_module'] == ['emailapp.signals']
            assert spoolqueue.spoolqueue.data['sender'] == ['webapp__Person']
            assert spoolqueue.spoolqueue.data['instance'] == [str(person.id)]
            assert spoolqueue.spoolqueue.data['created'] == ['1']
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
            assert simplejson.loads(spoolqueue.spoolqueue.data['kwargs'][0])['contacts_id'] == expected_contacts_id
        finally:
            views.get_spoolqueue = oldview

    def test_dispatcher_sends_via_http(self):
        from django.dispatch import dispatcher
        from django.db.models import signals
        from sigasync import sigasync_handler
        _OLD_HANDLER = sigasync_handler.HANDLE_VIA_HTTP
        sigasync_handler.HANDLE_VIA_HTTP = ['test']
        try:
            import pdb
            pdb.set_trace()
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
                dispatcher.send(instance=person, sender=Person,
                    signal=test_signal)
                assert data['spooler'] == 'default'
                assert data['data']['instance'] == str(person.id)
        finally:
            sigasync_handler.HANDLE_VIA_HTTP = _OLD_HANDLER


test_signal = object()

def http_test_handler(sender, instance, **kwargs):
    raise Exception('I SHOULD NOT BE CALLED')


# End
