from django.urls import path
from . import views

urlpatterns = [
    # 1. VISÕES PRINCIPAIS (Receber e Pagar)
    # A rota vazia '' agora aponta para Receber (antigo Fluxo Geral)
    path('', views.ContasReceberListView.as_view(), name='financeiro_lista'), 
    path('receber/', views.ContasReceberListView.as_view(), name='contas_receber'),
    path('pagar/', views.ContasPagarListView.as_view(), name='contas_pagar'),

    # 2. LANÇAMENTO DE DESPESAS
    path('despesa/nova/', views.DespesaCreateView.as_view(), name='despesa_create'),
    path('baixar/<int:pk>/', views.baixar_lancamento, name='baixar_lancamento'),

    # 3. CADASTROS (Fornecedores, Categorias, Contas)
    path('fornecedores/', views.FornecedorListView.as_view(), name='fornecedor_list'),
    path('fornecedores/novo/', views.FornecedorCreateView.as_view(), name='fornecedor_create'),

    path('config/categorias/', views.CategoriaListView.as_view(), name='categoria_list'),
    path('config/categorias/nova/', views.CategoriaCreateView.as_view(), name='categoria_create'),
    
    path('config/contas/', views.ContaListView.as_view(), name='conta_list'),
    path('config/contas/nova/', views.ContaCreateView.as_view(), name='conta_create'),
]