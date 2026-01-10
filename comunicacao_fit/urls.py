from django.urls import path
from . import views

app_name = 'comunicacao'

urlpatterns = [
    path('configuracoes/', views.whatsapp_dashboard, name='whatsapp_dashboard'),
    path('template/novo/', views.template_edit, name='template_create'),
    path('template/editar/<int:pk>/', views.template_edit, name='template_edit'),
    path('enviar-cobranca/<int:aluno_id>/', views.disparar_cobranca_manual, name='enviar_cobranca_whatsapp'),
]