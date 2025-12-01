from django.contrib import admin
from .models import CategoriaFinanceira, ContaBancaria, Lancamento

@admin.register(CategoriaFinanceira)
class CategoriaAdmin(admin.ModelAdmin):
    list_display = ['nome', 'tipo', 'categoria_pai']
    list_filter = ['tipo']

@admin.register(ContaBancaria)
class ContaAdmin(admin.ModelAdmin):
    list_display = ['nome', 'saldo_atual']

@admin.register(Lancamento)
class LancamentoAdmin(admin.ModelAdmin):
    list_display = ['data_vencimento', 'descricao', 'valor', 'status', 'categoria']
    list_filter = ['status', 'categoria__tipo', 'data_vencimento']
    search_fields = ['descricao']