from django.urls import include, path
from rest_framework.routers import DefaultRouter

from . import api_views

router = DefaultRouter()
router.register(r"otp", api_views.SendOTPViewSet, basename="otp")

urlpatterns = [
    path("v1/", include(router.urls)),
]
