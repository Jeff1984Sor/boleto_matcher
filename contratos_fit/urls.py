from django.urls import path
from . import views



urlpatterns = [
    # GESTÃO DE CONTRATOS (Lista de contratos feitos para alunos)
    path('lista/', views.ContratoListView.as_view(), name='contrato_list'), 
    path('novo/<int:aluno_id>/', views.novo_contrato, name='novo_contrato'),
    path('editar/<int:pk>/', views.ContratoUpdateView.as_view(), name='contrato_update'),
    path('excluir/<int:pk>/', views.ContratoDeleteView.as_view(), name='contrato_delete'),
    path('encerrar/<int:pk>/', views.encerrar_contrato, name='contrato_encerrar'),
    path('imprimir/<int:pk>/', views.imprimir_contrato, name='imprimir_contrato'),
    path('assinar/<uuid:token>/', views.assinar_contrato_view, name='assinar_contrato'),
    path('enviar-email/<int:pk>/', views.enviar_contrato_email, name='enviar_contrato_email'),

    # PLANOS E PREÇOS (O catálogo de planos do estúdio)
    path('planos/', views.PlanoListView.as_view(), name='plano_list'),
    path('planos/novo/', views.PlanoCreateView.as_view(), name='plano_create'),
    path('planos/editar/<int:pk>/', views.PlanoUpdateView.as_view(), name='plano_update'),
    path('planos/excluir/<int:pk>/', views.PlanoDeleteView.as_view(), name='plano_delete'),

    # MODELOS DE CONTRATO (Os templates/textos dos contratos)
    # Mudei de 'template_list' para 'modelo_list' para bater com a sidebar
    path('templates/', views.TemplateListView.as_view(), name='modelo_list'),
    path('templates/novo/', views.TemplateCreateView.as_view(), name='modelo_create'),
    path('templates/editar/<int:pk>/', views.TemplateEditorView.as_view(), name='modelo_update'),

    # FILTROS POR ALUNO
    path('aluno/<int:aluno_id>/', views.lista_contratos_aluno, name='lista_contratos_aluno'),
]