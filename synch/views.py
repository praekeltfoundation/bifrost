from django.http import HttpResponse


def health(_):
    return HttpResponse("OK")
