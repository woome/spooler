from webserviceapp import webservice
from sigasync.sigasync_spooler import SigAsyncSpool

class HttpSpool(SigAsyncSpool):
    def submit(self, spec):
        webservice.post('/spooler/%s/' % self._name, spec)
