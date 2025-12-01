from django.urls import path
from . import views

urlpatterns = [
    path('novo/<int:aluno_id>/', views.novo_contrato, name='novo_contrato'),
    path('imprimir/<int:pk>/', views.imprimir_contrato, name='imprimir_contrato'),

    # PLANOS
    path('planos/', views.PlanoListView.as_view(), name='plano_list'),
    path('planos/novo/', views.PlanoCreateView.as_view(), name='plano_create'),
    path('planos/editar/<int:pk>/', views.PlanoUpdateView.as_view(), name='plano_update'),
    path('planos/excluir/<int:pk>/', views.PlanoDeleteView.as_view(), name='plano_delete'),

    path('aluno/<int:aluno_id>/', views.lista_contratos_aluno, name='lista_contratos_aluno'),
]