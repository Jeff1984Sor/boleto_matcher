from django.urls import path
from . import views

urlpatterns = [
    # 1. Calendário Geral
    path('semanal/', views.calendario_semanal, name='calendario_semanal'),
    
    # 2. Agenda Específica do Aluno (Histórico)
    path('aluno/<int:aluno_id>/', views.lista_aulas_aluno, name='lista_aulas_aluno'),

    # 3. Gerenciamento da Aula (Professor)
    path('aula/<int:aula_id>/gerenciar/', views.gerenciar_aula, name='gerenciar_aula'),

    # 4. Ações Rápidas (Botões do Template)
    # Observe que os 'names' aqui devem ser iguais aos usados no {% url %} do HTML
    path('presenca/<int:pk>/confirmar/', views.confirmar_presenca, name='confirmar_presenca'),
    path('presenca/<int:pk>/cancelar/', views.cancelar_presenca, name='cancelar_presenca'),
    path('presenca/<int:pk>/remarcar/', views.remarcar_aula, name='remarcar_aula'),

    # 5. Relatórios e APIs
    path('relatorios/frequencia/', views.RelatorioFrequenciaView.as_view(), name='relatorio_frequencia'),
    path('api/totalpass/checkin/', views.checkin_totalpass, name='api_totalpass_checkin'),
    path('configuracao/integracao/', views.ConfiguracaoIntegracaoView.as_view(), name='config_integracao'),
    path('dashboard/', views.DashboardAulasView.as_view(), name='dashboard_aulas'),
    path('api/n8n/agenda-diaria/', views.api_agenda_amanha, name='api_agenda_amanha'),
    path('performance/', views.performance_aulas, name='performance_aulas'),
]