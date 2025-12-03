from django.shortcuts import render, get_object_or_404, redirect
from django.views.generic import ListView, CreateView, UpdateView, DeleteView
from django.urls import reverse_lazy
from django.contrib.auth.mixins import LoginRequiredMixin
from django.utils import timezone
from django.contrib import messages
from django.db.models import Sum
from django.http import HttpResponseRedirect
import uuid
from dateutil.relativedelta import relativedelta
from django.views.generic import DetailView

# Imports Locais
from .models import Lancamento, CategoriaFinanceira, ContaBancaria, Fornecedor
from .forms import CategoriaForm, ContaBancariaForm, DespesaForm, FornecedorForm
from cadastros_fit.models import Aluno

# ==============================================================================
# 1. CONTAS A RECEBER (ANTIGO FLUXO DE CAIXA GERAL)
# ==============================================================================

class ContasReceberListView(LoginRequiredMixin, ListView):
    model = Lancamento
    template_name = 'financeiro_fit/contas_receber.html'
    context_object_name = 'lancamentos'
    paginate_by = 50 

    def get_queryset(self):
        # Filtra apenas RECEITAS
        qs = super().get_queryset().filter(categoria__tipo='RECEITA')
        
        status = self.request.GET.get('status')
        inicio = self.request.GET.get('inicio')
        fim = self.request.GET.get('fim')
        aluno_id = self.request.GET.get('aluno_id')
        
        if status: qs = qs.filter(status=status)
        if inicio and fim: qs = qs.filter(data_vencimento__range=[inicio, fim])
        if aluno_id: qs = qs.filter(aluno__id=aluno_id)
            
        return qs.order_by('status', 'data_vencimento')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['hoje'] = timezone.now().date()
        
        # Totais
        qs = self.get_queryset()
        context['total_recebido'] = qs.filter(status='PAGO').aggregate(Sum('valor'))['valor__sum'] or 0
        context['total_pendente'] = qs.filter(status='PENDENTE').aggregate(Sum('valor'))['valor__sum'] or 0
        
        # Filtro Aluno
        if self.request.GET.get('aluno_id'):
            context['aluno_filtro'] = Aluno.objects.filter(pk=self.request.GET.get('aluno_id')).first()
            
        return context

# ==============================================================================
# 2. CONTAS A PAGAR (NOVA TELA)
# ==============================================================================

class ContasPagarListView(LoginRequiredMixin, ListView):
    model = Lancamento
    template_name = 'financeiro_fit/contas_pagar.html'
    context_object_name = 'lancamentos'
    paginate_by = 50

    def get_queryset(self):
        # Filtra apenas DESPESAS
        qs = super().get_queryset().filter(categoria__tipo='DESPESA')
        
        status = self.request.GET.get('status')
        inicio = self.request.GET.get('inicio')
        fim = self.request.GET.get('fim')
        fornecedor = self.request.GET.get('fornecedor')
        
        if status: qs = qs.filter(status=status)
        if inicio and fim: qs = qs.filter(data_vencimento__range=[inicio, fim])
        if fornecedor: qs = qs.filter(fornecedor_id=fornecedor)
            
        return qs.order_by('status', 'data_vencimento')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['hoje'] = timezone.now().date()
        
        # Lista de Fornecedores para o Filtro
        context['fornecedores'] = Fornecedor.objects.filter(ativo=True)
        
        # Totais
        qs = self.get_queryset()
        context['total_pago'] = qs.filter(status='PAGO').aggregate(Sum('valor'))['valor__sum'] or 0
        context['total_pendente'] = qs.filter(status='PENDENTE').aggregate(Sum('valor'))['valor__sum'] or 0
        
        return context

class DespesaCreateView(LoginRequiredMixin, CreateView):
    model = Lancamento
    form_class = DespesaForm
    template_name = 'financeiro_fit/despesa_form.html'
    success_url = reverse_lazy('contas_pagar')

    def form_valid(self, form):
        # --- LÓGICA DE RECORRÊNCIA ---
        dados = form.save(commit=False)
        # Se você removeu 'organizacao' do model, apague esta linha:
        # dados.organizacao = self.request.tenant 
        
        repetir = form.cleaned_data.get('repetir')
        frequencia = form.cleaned_data.get('frequencia')
        qtd = form.cleaned_data.get('qtd_repeticoes') or 1

        if repetir and qtd > 1:
            grupo_id = uuid.uuid4()
            
            for i in range(qtd):
                nova_despesa = Lancamento(
                    # Se removeu organizacao, apague aqui tb
                    # organizacao=self.request.tenant, 
                    descricao=f"{dados.descricao} ({i+1}/{qtd})",
                    fornecedor=dados.fornecedor,
                    profissional=dados.profissional,
                    categoria=dados.categoria,
                    conta=dados.conta,
                    valor=dados.valor,
                    arquivo_boleto=dados.arquivo_boleto,
                    grupo_serie=grupo_id,
                    status='PENDENTE'
                )
                
                # Calcula Vencimento
                base_date = dados.data_vencimento
                if frequencia == 'MENSAL':
                    nova_despesa.data_vencimento = base_date + relativedelta(months=i)
                elif frequencia == 'SEMANAL':
                    nova_despesa.data_vencimento = base_date + relativedelta(weeks=i)
                elif frequencia == 'ANUAL':
                    nova_despesa.data_vencimento = base_date + relativedelta(years=i)
                else: 
                    nova_despesa.data_vencimento = base_date # Caso não selecione, salva na mesma data (ou + dias)
                
                nova_despesa.save()
            
            messages.success(self.request, f"{qtd} despesas geradas com sucesso!")
            return redirect(self.success_url)
        
        else:
            # Salva normal (única)
            return super().form_valid(form)

# ==============================================================================
# 3. AÇÕES (BAIXA)
# ==============================================================================

def baixar_lancamento(request, pk):
    lancamento = get_object_or_404(Lancamento, pk=pk)
    
    if request.method == 'POST':
        lancamento.status = 'PAGO'
        lancamento.data_pagamento = timezone.now().date()
        lancamento.forma_pagamento = request.POST.get('forma_pagamento')
        lancamento.save()
        
        # Atualiza Saldo da Conta
        if lancamento.conta:
            if lancamento.categoria.tipo == 'RECEITA':
                lancamento.conta.saldo_atual += lancamento.valor
            else:
                lancamento.conta.saldo_atual -= lancamento.valor
            lancamento.conta.save()
        
        messages.success(request, "Baixa realizada!")
        
    # Volta pra onde estava (Receber ou Pagar)
    next_url = request.META.get('HTTP_REFERER')
    if next_url: return HttpResponseRedirect(next_url)
    return redirect('contas_receber')

# ==============================================================================
# 4. CADASTROS AUXILIARES
# ==============================================================================

# Fornecedores
class FornecedorListView(LoginRequiredMixin, ListView):
    model = Fornecedor
    template_name = 'financeiro_fit/fornecedor_list.html'
    context_object_name = 'fornecedores'

class FornecedorCreateView(LoginRequiredMixin, CreateView):
    model = Fornecedor
    form_class = FornecedorForm
    template_name = 'financeiro_fit/form_generico.html'
    success_url = reverse_lazy('fornecedor_list')

# Categorias
class CategoriaListView(LoginRequiredMixin, ListView):
    model = CategoriaFinanceira
    template_name = 'financeiro_fit/categoria_list.html'
    context_object_name = 'categorias'

class CategoriaCreateView(LoginRequiredMixin, CreateView):
    model = CategoriaFinanceira
    form_class = CategoriaForm
    template_name = 'financeiro_fit/form_generico.html'
    success_url = reverse_lazy('categoria_list')

# Contas Bancárias
class ContaListView(LoginRequiredMixin, ListView):
    model = ContaBancaria
    template_name = 'financeiro_fit/conta_list.html'
    context_object_name = 'contas'

class ContaCreateView(LoginRequiredMixin, CreateView):
    model = ContaBancaria
    form_class = ContaBancariaForm
    template_name = 'financeiro_fit/form_generico.html'
    success_url = reverse_lazy('conta_list')


class ContaExtratoView(LoginRequiredMixin, DetailView):
    model = ContaBancaria
    template_name = 'financeiro_fit/conta_extrato.html'
    context_object_name = 'conta'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Filtros de Data (Padrão: Últimos 30 dias se não informado)
        inicio = self.request.GET.get('inicio')
        fim = self.request.GET.get('fim')
        
        # Pega lançamentos PAGOS desta conta
        lancamentos = Lancamento.objects.filter(
            conta=self.object, 
            status='PAGO'
        ).order_by('-data_pagamento') # Mais recentes primeiro
        
        if inicio and fim:
            lancamentos = lancamentos.filter(data_pagamento__range=[inicio, fim])
            
        context['lancamentos'] = lancamentos
        
        # Resumo do Período
        entradas = lancamentos.filter(categoria__tipo='RECEITA').aggregate(Sum('valor'))['valor__sum'] or 0
        saidas = lancamentos.filter(categoria__tipo='DESPESA').aggregate(Sum('valor'))['valor__sum'] or 0
        
        context['total_entradas'] = entradas
        context['total_saidas'] = saidas
        context['resultado_periodo'] = entradas - saidas
        
        return context