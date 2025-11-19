from django.urls import path
from . import views

urlpatterns = [
    path('', views.gerador_home, name='pdf_home'),
]