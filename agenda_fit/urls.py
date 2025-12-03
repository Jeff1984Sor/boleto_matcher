from django.urls import path
from . import views

urlpatterns = [
    path('semanal/', views.calendario_semanal, name='calendario_semanal'),
    path('aula/<int:aula_id>/gerenciar/', views.gerenciar_aula, name='gerenciar_aula'),
    path('aluno/<int:aluno_id>/', views.lista_aulas_aluno, name='lista_aulas_aluno'),

    path('acao/presenca/<int:presenca_id>/', views.acao_marcar_presenca, name='acao_marcar_presenca'),
    path('acao/realizada/<int:presenca_id>/', views.acao_marcar_realizada, name='acao_marcar_realizada'),
    path('acao/deletar/<int:presenca_id>/', views.acao_deletar_agendamento, name='acao_deletar_agendamento'),
    path('acao/remarcar/<int:presenca_id>/', views.acao_remarcar_aula, name='acao_remarcar_aula'),

    path('api/totalpass/checkin/', views.checkin_totalpass, name='api_totalpass_checkin'),
    path('aulas/aluno/<int:aluno_id>/', views.lista_aulas_aluno, name='lista_aulas_aluno'),
    path('relatorios/frequencia/', views.RelatorioFrequenciaView.as_view(), name='relatorio_frequencia'),
    path('aluno/<int:aluno_id>/', views.lista_aulas_aluno, name='lista_aulas_aluno'),
]