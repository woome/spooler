

"""Views for sigasync - test stuff only right now"""

from django.db import transaction
from django.http import HttpRequest
from django.http import HttpResponse
from django.http import HttpResponseServerError
from models import SigasyncTest1
from models import SigasyncTest2

import pdb

DEFAULT_QUEUE_LABEL="x"

@transaction.commit_manually
def test_1(request):
    if request.method == "POST":
        s = SigasyncTest1(queue_label=request.POST.get("queuelabel", DEFAULT_QUEUE_LABEL))
        s.save()
        transaction.commit()
        return HttpResponse("%d" % s.id)

    return HttpResponse()

def test_2(request, sigasync_test1_id):
    try:
        test_obj1 = SigasyncTest1.objects.get(id=sigasync_test1_id)
        test_obj2 = SigasyncTest2.objects.get(test1=test_obj1)
        return HttpResponse("%d" % test_obj2.test1.id)
    except SigasyncTest2.DoesNotExist:
        return HttpResponseServerError()


# End
