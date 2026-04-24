"""
Project-level URL configuration.
File location: nifty100/urls.py
"""

from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path("admin/",  admin.site.urls),
    path("api/",    include("intelligence.urls")),
]