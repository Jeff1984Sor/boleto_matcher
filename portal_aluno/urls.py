from django.urls import path
from . import views

app_name = 'portalaluno'

urlpatterns = [
    path('', views.dashboard, name='aluno_dashboard'),
    path('agenda/', views.minha_agenda, name='aluno_agenda'),
    path('financeiro/', views.meu_financeiro, name='aluno_financeiro'),
    path('marcar/<int:aula_id>/', views.marcar_aula, name='aluno_marcar'),
    path('cancelar/<int:aula_id>/', views.cancelar_aula, name='aluno_cancelar'),
]