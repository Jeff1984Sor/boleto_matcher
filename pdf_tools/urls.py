from django.urls import path
from . import views

urlpatterns = [
    path('', views.gerador_home, name='pdf_home'),
    path('api/upload/', views.api_upload_arquivo, name='api_upload'),
    path('api/delete/', views.api_delete_arquivo, name='api_delete'),
    path('api/processar/', views.api_iniciar_processamento, name='api_processar'),
]