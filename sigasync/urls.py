from django.conf.urls.defaults import *

urlpatterns = patterns('sigasync.views',
    (r'^(?P<spooler>.+)/$', 'spooler_http_gateway'),
)

