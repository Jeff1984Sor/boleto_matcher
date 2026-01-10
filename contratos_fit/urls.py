from django.urls import path
from . import views

app_name = 'contratos'

urlpatterns = [
    path('novo/<int:aluno_id>/', views.novo_contrato, name='novo_contrato'),
    path('imprimir/<int:pk>/', views.imprimir_contrato, name='imprimir_contrato'),

    # PLANOS
    path('planos/', views.PlanoListView.as_view(), name='plano_list'),
    path('planos/novo/', views.PlanoCreateView.as_view(), name='plano_create'),
    path('planos/editar/<int:pk>/', views.PlanoUpdateView.as_view(), name='plano_update'),
    path('planos/excluir/<int:pk>/', views.PlanoDeleteView.as_view(), name='plano_delete'),

    path('aluno/<int:aluno_id>/', views.lista_contratos_aluno, name='lista_contratos_aluno'),
    path('lista/', views.ContratoListView.as_view(), name='contrato_list'),

    path('editar/<int:pk>/', views.ContratoUpdateView.as_view(), name='contrato_update'),
    path('excluir/<int:pk>/', views.ContratoDeleteView.as_view(), name='contrato_delete'),
    path('encerrar/<int:pk>/', views.encerrar_contrato, name='contrato_encerrar'),

    path('templates/', views.TemplateListView.as_view(), name='template_list'),
    path('templates/novo/', views.TemplateCreateView.as_view(), name='template_create'),
    path('templates/editar/<int:pk>/', views.TemplateEditorView.as_view(), name='template_update'),

    path('contratos/aluno/<int:aluno_id>/', views.lista_contratos_aluno, name='lista_contratos_aluno'),
    path('assinar/<uuid:token>/', views.assinar_contrato_view, name='assinar_contrato'),
    path('enviar-email/<int:pk>/', views.enviar_contrato_email, name='enviar_contrato_email'),


]