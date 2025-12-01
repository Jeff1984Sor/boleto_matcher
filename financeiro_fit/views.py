from django.shortcuts import render

# Create your views here.
from django.shortcuts import render, get_object_or_404, redirect
from django.views.generic import ListView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.utils import timezone
from django.contrib import messages
from django.db.models import Sum

from .models import Lancamento, ContaBancaria

class LancamentoListView(LoginRequiredMixin, ListView):
    model = Lancamento
    template_name = 'financeiro_fit/lancamento_list.html'
    context_object_name = 'lancamentos'
    paginate_by = 20

    def get_queryset(self):
        queryset = super().get_queryset()
        
        # Filtros da URL
        status = self.request.GET.get('status')
        inicio = self.request.GET.get('inicio')
        fim = self.request.GET.get('fim')
        aluno_id = self.request.GET.get('aluno_id')  # <--- NOVO
        
        if status:
            queryset = queryset.filter(status=status)
        
        if inicio and fim:
            queryset = queryset.filter(data_vencimento__range=[inicio, fim])
            
        if aluno_id:  # <--- LÓGICA NOVA
            queryset = queryset.filter(aluno__id=aluno_id)
            
        return queryset.order_by('data_vencimento')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Totais para o cabeçalho
        qs = self.get_queryset()
        context['total_receitas'] = qs.filter(categoria__tipo='RECEITA').aggregate(Sum('valor'))['valor__sum'] or 0
        context['total_despesas'] = qs.filter(categoria__tipo='DESPESA').aggregate(Sum('valor'))['valor__sum'] or 0
        return context

# Função para Receber/Pagar (Dar Baixa)
def baixar_lancamento(request, pk):
    lancamento = get_object_or_404(Lancamento, pk=pk)
    
    if request.method == 'POST':
        # 1. Atualiza o Lançamento
        lancamento.status = 'PAGO'
        lancamento.data_pagamento = timezone.now().date()
        lancamento.forma_pagamento = request.POST.get('forma_pagamento')
        lancamento.save()
        
        # 2. Atualiza o Saldo da Conta Bancária
        conta = lancamento.conta
        if lancamento.categoria.tipo == 'RECEITA':
            conta.saldo_atual += lancamento.valor
        else:
            conta.saldo_atual -= lancamento.valor
        conta.save()
        
        messages.success(request, f"Lançamento '{lancamento.descricao}' baixado com sucesso!")
        
    return redirect('financeiro_lista')