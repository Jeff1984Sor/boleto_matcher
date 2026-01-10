from django.urls import path
from . import views

app_name = 'financeiro'

urlpatterns = [
    # 1. VISÕES PRINCIPAIS (Listagens)
    # Rota raiz do financeiro agora cai em Contas a Receber por padrão
    path('', views.ContasReceberListView.as_view(), name='financeiro_lista'), 
    path('receber/', views.ContasReceberListView.as_view(), name='contas_receber'),
    path('pagar/', views.ContasPagarListView.as_view(), name='contas_pagar'),

    # 2. LANÇAMENTOS (Criação, Edição e Baixa)
    # Cadastro de Despesas (O formulário que vamos melhorar)
    path('pagar/novo/', views.DespesaCreateView.as_view(), name='despesa_create'),
    
    # Cadastro de Receitas Avulsas (Vendas manuais/extras)
    path('receber/nova/', views.ReceitaCreateView.as_view(), name='receita_create'),
    
    # Edição de qualquer lançamento
    path('lancamento/<int:pk>/editar/', views.LancamentoUpdateView.as_view(), name='lancamento_edit'),
    
    # Ação única de Baixa (Funciona para Receber e Pagar)
    path('baixar/<int:pk>/', views.baixar_lancamento, name='baixar_lancamento'),

    # 3. CADASTROS APOIO (Fornecedores, Categorias, Contas)
    path('fornecedores/', views.FornecedorListView.as_view(), name='fornecedor_list'),
    path('fornecedores/novo/', views.FornecedorCreateView.as_view(), name='fornecedor_create'),

    path('config/categorias/', views.CategoriaListView.as_view(), name='categoria_list'),
    path('config/categorias/nova/', views.CategoriaCreateView.as_view(), name='categoria_create'),
    
    # ESTAS DUAS LINHAS SÃO ESSENCIAIS:
    path('config/categorias/<int:pk>/editar/', views.CategoriaUpdateView.as_view(), name='categoria_update'),
    path('config/categorias/<int:pk>/excluir/', views.CategoriaDeleteView.as_view(), name='categoria_delete'),

    path('config/contas/', views.ContaListView.as_view(), name='conta_list'),
    path('config/contas/nova/', views.ContaCreateView.as_view(), name='conta_create'),

    # 4. EXTRATOS E RELATÓRIOS
    path('config/contas/<int:pk>/extrato/', views.ContaExtratoView.as_view(), name='conta_extrato'),
    path('contas/<int:pk>/exportar/excel/', views.exportar_extrato_excel, name='exportar_extrato_excel'),
    path('contas/<int:pk>/exportar/pdf/', views.exportar_extrato_pdf, name='exportar_extrato_pdf'),

    path('fornecedores/', views.FornecedorListView.as_view(), name='fornecedor_list'),
    path('fornecedores/novo/', views.FornecedorCreateView.as_view(), name='fornecedor_create'),
    path('fornecedores/<int:pk>/editar/', views.FornecedorUpdateView.as_view(), name='fornecedor_update'),

    # 5. DASHBOARD GERAL
    path('dashboard/', views.DashboardFinanceiroView.as_view(), name='dashboard_financeiro'),
    path('relatorio-dre/', views.relatorio_dre, name='relatorio_dre'),
]