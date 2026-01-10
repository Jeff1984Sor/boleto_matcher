from django.urls import path
from . import views


urlpatterns = [
    # --- ALUNOS ---
    path('alunos/', views.AlunoListView.as_view(), name='aluno_list'),
    path('alunos/novo/', views.AlunoCreateView.as_view(), name='aluno_create'),
    
    # NOVA ROTA: Ficha do Aluno (Detalhes)
    path('alunos/<int:pk>/', views.AlunoDetailView.as_view(), name='aluno_detail'),
    path('alunos/<int:pk>/documento/novo/', views.upload_documento_extra, name='upload_documento_extra'),
    
    # Rota de Edição (agora acessível via botão na ficha ou direto)
    path('alunos/<int:pk>/editar/', views.AlunoUpdateView.as_view(), name='aluno_update'),
    
    path('alunos/<int:pk>/deletar/', views.AlunoDeleteView.as_view(), name='aluno_delete'),

    # --- PROFISSIONAIS ---
    path('equipe/', views.ProfissionalListView.as_view(), name='profissional_list'),
    path('equipe/novo/', views.ProfissionalCreateView.as_view(), name='profissional_create'),
    path('equipe/<int:pk>/editar/', views.ProfissionalUpdateView.as_view(), name='profissional_update'),

    # --- UNIDADES ---
    path('unidades/', views.UnidadeListView.as_view(), name='unidade_list'),
    path('unidades/nova/', views.UnidadeCreateView.as_view(), name='unidade_create'),
    path('unidades/<int:pk>/editar/', views.UnidadeUpdateView.as_view(), name='unidade_update'),
    path('unidades/<int:pk>/deletar/', views.UnidadeDeleteView.as_view(), name='unidade_delete'),

    # --- API / AJAX ---
    path('api/ler-documento/', views.api_ler_documento, name='api_ler_documento'),
    path('api/n8n/agenda-diaria/', views.api_agenda_amanha, name='api_agenda_amanha'),

    

    path('servicos/', views.TipoServicoListView.as_view(), name='servico_list'),
    path('servicos/novo/', views.TipoServicoCreateView.as_view(), name='servico_create'),
]