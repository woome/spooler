from django.conf.urls.defaults import *

urlpatterns = patterns('sigasync.views',
    (r'^v1/$', 'test_1'),
    (r'^v2/(?P<sigasync_test1_id>[0-9]+)/$', 'test_2'),
)

# End
