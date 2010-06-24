from webserviceapp import webservice
from sigasync.sigasync_spooler import SigAsyncSpool
import logging

class HttpSpool(SigAsyncSpool):
    def submit(self, spec):
        logger = logging.getLogger("yesmail.http.submit")
        logger.warning("submitting job to /spooler/%s/" % self._name)
        webservice.post('/spooler/%s/' % self._name, spec)
