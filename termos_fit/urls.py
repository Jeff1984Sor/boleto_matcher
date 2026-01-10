from django.urls import path
from . import views

app_name = 'termos'

urlpatterns = [
    # Admin: Gerenciar Modelos
    path('modelos/', views.termo_template_list, name='termo_template_list'),
    path('modelos/novo/', views.termo_template_create, name='termo_template_create'),
    path('modelos/', views.termo_template_list, name='termo_template_list'),
    
    # Operacional: Gerar e Assinar
    path('gerar/<int:aluno_id>/', views.gerar_termo_aluno, name='gerar_termo_aluno'),
    path('assinar/<uuid:token>/', views.assinar_termo, name='assinar_termo'),
]