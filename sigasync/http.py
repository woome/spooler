from urllib import urlencode
from django.http import QueryDict
from httplib2 import Http
from sigasync_spooler import SigAsyncSpool, get_config

__all__ = ['WebService', 'HttpSpool']

class WebService(object):    
    def __init__(self, **kwargs):
        self.http = Http()
        self.endpoint = kwargs['webservice_endpoint']

    def _get_http_client(self):
        return self.http

    def _do_call(self, method, url, data={}, headers={}):
        # convert data dicts to x-www-form-urlencoded
        if isinstance(data, QueryDict):
            data = data.urlencode()
        elif isinstance(data, dict):
            data = urlencode(data, doseq=True)
        
        headers = headers.copy()
        
        if method == 'GET':
            url = "%s?%s" % (url, data)
        elif method == 'POST':
            headers['Content-type'] = 'application/x-www-form-urlencoded'
        
        url = '%s%s' % (self.endpoint, url)
        resp, body = self._get_http_client().request(url, method, body=data, headers = headers)
        return resp, body

    def post(self, url, data={}, headers={}):
        return self._do_call("POST", url, data, headers)

    def get(self, url, data={}, headers={}):
        return self._do_call("GET", url, data, headers)

class HttpSpool(SigAsyncSpool):
    def submit(self, spec):
        webservice = WebService(get_config())
        webservice.post('/spooler/%s/' % self._name, spec)
