from django.shortcuts import render, get_object_or_404, redirect
from django.views.generic import ListView, CreateView, UpdateView, DeleteView, DetailView, TemplateView
from django.urls import reverse_lazy
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.decorators import login_required
from django.utils import timezone
from django.contrib import messages
from django.db.models import Sum
from django.http import HttpResponseRedirect, HttpResponse
from django.template.loader import get_template
import calendar
from django.db.models import Q
from .models import Lancamento
import uuid
import csv
from dateutil.relativedelta import relativedelta
from xhtml2pdf import pisa
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from datetime import datetime

# Imports Locais
from .models import Lancamento, CategoriaFinanceira, ContaBancaria, Fornecedor
from .forms import CategoriaForm, ContaBancariaForm, DespesaForm, FornecedorForm
from cadastros_fit.models import Aluno

from django.db.models.functions import ExtractMonth
from contratos_fit.models import Contrato

# ==============================================================================
# 1. CONTAS A RECEBER (ANTIGO FLUXO DE CAIXA GERAL)
# ==============================================================================

class ContasReceberListView(LoginRequiredMixin, ListView):
    model = Lancamento
    template_name = 'financeiro_fit/contas_receber.html'
    context_object_name = 'lancamentos'
    paginate_by = 20

    def get_queryset(self):
        # Filtra apenas RECEITAS (Contas a Receber) da organização atual
        queryset = Lancamento.objects.filter(categoria__tipo='RECEITA').order_by('data_vencimento')

        # Captura os filtros do GET
        aluno_nome = self.request.GET.get('aluno')
        status = self.request.GET.get('status')
        data_inicio = self.request.GET.get('data_inicio')
        data_fim = self.request.GET.get('data_fim')

        # Aplica os filtros se existirem
        if aluno_nome:
            queryset = queryset.filter(aluno__nome__icontains=aluno_nome)
        
        if status:
            queryset = queryset.filter(status=status)
        
        if data_inicio:
            queryset = queryset.filter(data_vencimento__gte=data_inicio)
        
        if data_fim:
            queryset = queryset.filter(data_vencimento__lte=data_fim)

        return queryset

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['alunos_list'] = Aluno.objects.filter(ativo=True).order_by('nome')
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
        context['fornecedores'] = Fornecedor.objects.filter(ativo=True)
        
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
        # dados.organizacao = self.request.tenant  (Se não tiver o campo no model, mantenha comentado)
        
        repetir = form.cleaned_data.get('repetir')
        frequencia = form.cleaned_data.get('frequencia')
        qtd = form.cleaned_data.get('qtd_repeticoes') or 1

        if repetir and qtd > 1:
            grupo_id = uuid.uuid4()
            
            for i in range(qtd):
                nova_despesa = Lancamento(
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
                
                base_date = dados.data_vencimento
                if frequencia == 'MENSAL':
                    nova_despesa.data_vencimento = base_date + relativedelta(months=i)
                elif frequencia == 'SEMANAL':
                    nova_despesa.data_vencimento = base_date + relativedelta(weeks=i)
                elif frequencia == 'ANUAL':
                    nova_despesa.data_vencimento = base_date + relativedelta(years=i)
                else: 
                    nova_despesa.data_vencimento = base_date
                
                nova_despesa.save()
            
            messages.success(self.request, f"{qtd} despesas geradas com sucesso!")
            return redirect(self.success_url)
        
        else:
            return super().form_valid(form)

# ==============================================================================
# 3. AÇÕES (BAIXA)
# ==============================================================================

@login_required
def baixar_lancamento(request, pk):
    lancamento = get_object_or_404(Lancamento, pk=pk)
    
    if request.method == 'POST':
        # Pega os dados do modal
        data_pagto = request.POST.get('data_pagamento')
        obs = request.POST.get('observacao')
        forma = request.POST.get('forma_pagamento', 'PIX')

        lancamento.status = 'PAGO'
        lancamento.data_pagamento = data_pagto if data_pagto else timezone.now().date()
        lancamento.observacao = obs
        lancamento.forma_pagamento = forma
        lancamento.save()
        
        # Atualiza o saldo bancário
        if lancamento.conta:
            if lancamento.categoria.tipo == 'RECEITA':
                lancamento.conta.saldo_atual += lancamento.valor
            else:
                lancamento.conta.saldo_atual -= lancamento.valor
            lancamento.conta.save()
        
        messages.success(request, f"Recebimento de {lancamento.aluno.nome} confirmado!")
        
    return redirect(request.META.get('HTTP_REFERER', 'contas_receber'))

@login_required
def estornar_lancamento(request, pk):
    """ Reverte um lançamento PAGO para PENDENTE """
    lancamento = get_object_or_404(Lancamento, pk=pk)
    
    if request.method == 'POST' and lancamento.status == 'PAGO':
        # Remove o valor do saldo bancário (revertendo)
        if lancamento.conta:
            if lancamento.categoria.tipo == 'RECEITA':
                lancamento.conta.saldo_atual -= lancamento.valor
            else:
                lancamento.conta.saldo_atual += lancamento.valor
            lancamento.conta.save()

        # Reseta os campos
        lancamento.status = 'PENDENTE'
        lancamento.data_pagamento = None
        # Mantemos a observação mas avisamos que foi estornado
        lancamento.observacao = f"[ESTORNADO] {lancamento.observacao or ''}"
        lancamento.save()
        
        messages.warning(request, "Lançamento estornado com sucesso!")
        
    return redirect(request.META.get('HTTP_REFERER', 'contas_receber'))

# ==============================================================================
# 4. CADASTROS AUXILIARES
# ==============================================================================

class FornecedorListView(LoginRequiredMixin, ListView):
    model = Fornecedor
    template_name = 'financeiro_fit/fornecedor_list.html'
    context_object_name = 'fornecedores'

class FornecedorCreateView(LoginRequiredMixin, CreateView):
    model = Fornecedor
    form_class = FornecedorForm
    template_name = 'financeiro_fit/fornecedor_form.html'
    success_url = reverse_lazy('fornecedor_list')

class CategoriaListView(LoginRequiredMixin, ListView):
    model = CategoriaFinanceira
    template_name = 'financeiro_fit/categoria_list.html'
    context_object_name = 'categorias'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Função auxiliar para montar a árvore (Pai -> Filho -> Neto)
        def organizar_arvore(tipo):
            lista_ordenada = []
            
            # 1. Pega os PAIS (Nível 0) - aqueles que não tem pai
            pais = CategoriaFinanceira.objects.filter(
                tipo=tipo, 
                categoria_pai__isnull=True
            ).order_by('nome')

            for pai in pais:
                pai.nivel = 0 # Define o nível visual
                lista_ordenada.append(pai)
                
                # 2. Pega os FILHOS (Nível 1)
                filhos = CategoriaFinanceira.objects.filter(categoria_pai=pai).order_by('nome')
                for filho in filhos:
                    filho.nivel = 1
                    lista_ordenada.append(filho)
                    
                    # 3. Pega os NETOS (Nível 2)
                    netos = CategoriaFinanceira.objects.filter(categoria_pai=filho).order_by('nome')
                    for neto in netos:
                        neto.nivel = 2
                        lista_ordenada.append(neto)
            
            return lista_ordenada

        # Separa as listas para exibir organizado na tela
        context['arvore_receitas'] = organizar_arvore('RECEITA')
        context['arvore_despesas'] = organizar_arvore('DESPESA')
        
        return context

class CategoriaCreateView(LoginRequiredMixin, CreateView):
    model = CategoriaFinanceira
    form_class = CategoriaForm
    template_name = 'financeiro_fit/form_generico.html'
    success_url = reverse_lazy('categoria_list')

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
        # Filtros de data vindos da URL (?data_inicio=...&data_fim=...)
        data_inicio = self.request.GET.get('data_inicio')
        data_fim = self.request.GET.get('data_fim')
        
        # Busca lançamentos desta conta
        lancamentos = Lancamento.objects.filter(conta=self.object).order_by('-data_vencimento')
        
        if data_inicio:
            lancamentos = lancamentos.filter(data_vencimento__gte=data_inicio)
        if data_fim:
            lancamentos = lancamentos.filter(data_vencimento__lte=data_fim)
            
        context['lancamentos'] = lancamentos
        context['filtros'] = self.request.GET # Para manter as datas nos campos do form
        return context
# ==============================================================================
# 5. EXPORTAÇÃO (EXCEL E PDF)
# ==============================================================================
    
def exportar_extrato_excel(request, pk):
    conta = get_object_or_404(ContaBancaria, pk=pk)
    lancamentos = Lancamento.objects.filter(conta=conta).order_by('data_vencimento')
    
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = f"Extrato - {conta.nome}"
    
    # Cabeçalho
    columns = ['Data', 'Descrição', 'Aluno/Fornecedor', 'Valor', 'Status']
    ws.append(columns)
    
    for l in lancamentos:
        ws.append([
            l.data_vencimento.strftime('%d/%m/%Y'),
            l.descricao,
            l.aluno.nome if l.aluno else (l.fornecedor.nome if l.fornecedor else ""),
            float(l.valor),
            l.status
        ])
    
    response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = f'attachment; filename=extrato_{conta.nome}.xlsx'
    wb.save(response)
    return response

# --- EXPORTAR PDF ---
def exportar_extrato_pdf(request, pk):
    conta = get_object_or_404(ContaBancaria, pk=pk)
    lancamentos = Lancamento.objects.filter(conta=conta).order_by('data_vencimento')
    
    template_path = 'financeiro_fit/extrato_pdf_template.html'
    context = {'conta': conta, 'lancamentos': lancamentos, 'hoje': timezone.now()}
    
    response = HttpResponse(content_type='application/pdf')
    response['Content-Disposition'] = f'attachment; filename="extrato_{conta.nome}.pdf"'
    
    template = get_template(template_path)
    html = template.render(context)
    
    pisa_status = pisa.CreatePDF(html, dest=response)
    if pisa_status.err:
        return HttpResponse('Erro ao gerar PDF', status=500)
    return response

class DashboardFinanceiroView(LoginRequiredMixin, TemplateView):
    template_name = 'financeiro_fit/dashboard_financeiro.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        hoje = timezone.now()
        ano = int(self.request.GET.get('ano', hoje.year))
        mes = int(self.request.GET.get('mes', hoje.month))
        
        _, last_day = calendar.monthrange(ano, mes)
        inicio_mes = timezone.datetime(ano, mes, 1).date()
        fim_mes = timezone.datetime(ano, mes, last_day).date()

        context['ano_atual'] = ano
        context['mes_atual'] = mes
        
        # 1. Totais do Mês
        lancamentos_mes = Lancamento.objects.filter(data_vencimento__range=[inicio_mes, fim_mes])
        
        receitas = lancamentos_mes.filter(categoria__tipo='RECEITA', status='PAGO').aggregate(Sum('valor'))['valor__sum'] or 0
        despesas = lancamentos_mes.filter(categoria__tipo='DESPESA', status='PAGO').aggregate(Sum('valor'))['valor__sum'] or 0
        context['receita_mes'] = receitas
        context['despesa_mes'] = despesas
        context['resultado_mes'] = receitas - despesas

        # 2. Gráfico Mês a Mês (Ano Todo)
        # Pega dados do ano inteiro agrupados por mês
        dados_ano = Lancamento.objects.filter(data_vencimento__year=ano, status='PAGO') \
            .annotate(mes=ExtractMonth('data_vencimento')) \
            .values('mes', 'categoria__tipo') \
            .annotate(total=Sum('valor'))
            
        # Prepara arrays para o Chart.js (12 posições zeradas)
        receita_anual = [0] * 12
        despesa_anual = [0] * 12
        
        for d in dados_ano:
            idx = d['mes'] - 1
            if d['categoria__tipo'] == 'RECEITA':
                receita_anual[idx] = float(d['total'])
            else:
                despesa_anual[idx] = float(d['total'])
                
        context['chart_receita'] = receita_anual
        context['chart_despesa'] = despesa_anual

        # 3. Contratos Novos
        context['contratos_novos'] = Contrato.objects.filter(data_inicio__range=[inicio_mes, fim_mes])

        # 4. Contratos Vencendo
        context['contratos_vencendo'] = Contrato.objects.filter(data_fim__range=[inicio_mes, fim_mes])

        return context
    

class ReceitaCreateView(LoginRequiredMixin, CreateView):
    model = Lancamento
    template_name = 'financeiro_fit/lancamento_form.html' # Usa o mesmo form bonito
    fields = ['descricao', 'aluno', 'categoria', 'conta', 'valor', 'data_vencimento', 'status']
    success_url = reverse_lazy('contas_receber')

    def form_valid(self, form):
        # Garante que seja sempre uma Receita
        # Se sua categoria tiver o tipo, você pode forçar aqui
        return super().form_valid(form)

class LancamentoUpdateView(LoginRequiredMixin, UpdateView):
    model = Lancamento
    template_name = 'financeiro_fit/lancamento_form.html'
    fields = '__all__' # Ou especifique os campos
    success_url = reverse_lazy('financeiro_lista')

@login_required
def relatorio_dre(request):
    # 1. Pegar Mês e Ano do filtro (ou usar o atual como padrão)
    hoje = datetime.now()
    mes_selecionado = int(request.GET.get('mes', hoje.month))
    ano_selecionado = int(request.GET.get('ano', hoje.year))

    # 2. Filtrar Lançamentos PAGOS no período
    # Usamos o status 'PAGO' para ser um DRE de regime de caixa (o que realmente entrou/saiu)
    base_queryset = Lancamento.objects.filter(
        status='PAGO',
        data_vencimento__month=mes_selecionado,
        data_vencimento__year=ano_selecionado
    )

    # 3. Agrupar Receitas por Categoria
    dre_receitas = base_queryset.filter(categoria__tipo='RECEITA').values(
        'categoria__nome'
    ).annotate(
        total=Sum('valor')
    ).order_by('-total')

    # 4. Agrupar Despesas por Categoria
    dre_despesas = base_queryset.filter(categoria__tipo='DESPESA').values(
        'categoria__nome'
    ).annotate(
        total=Sum('valor')
    ).order_by('-total')

    # 5. Calcular Totais Finais
    total_receitas = sum(item['total'] for item in dre_receitas) or 0
    total_despesas = sum(item['total'] for item in dre_despesas) or 0
    lucro_liquido = total_receitas - total_despesas
    
    # 6. Calcular Margem de Lucro %
    margem_lucro = 0
    if total_receitas > 0:
        margem_lucro = (lucro_liquido / total_receitas) * 100

    context = {
        'dre_receitas': dre_receitas,
        'dre_despesas': dre_despesas,
        'total_receitas': total_receitas,
        'total_despesas': total_despesas,
        'lucro_liquido': lucro_liquido,
        'margem_lucro': round(margem_lucro, 1),
        'mes_sel': mes_selecionado,
        'ano_sel': ano_selecionado,
    }
    
    return render(request, 'financeiro_fit/relatorio_dre.html', context)
# Adicione esta classe na sua views.py
class CategoriaUpdateView(LoginRequiredMixin, UpdateView):
    model = CategoriaFinanceira
    form_class = CategoriaForm
    template_name = 'financeiro_fit/form_generico.html'
    success_url = reverse_lazy('categoria_list')

    def form_valid(self, form):
        messages.success(self.request, "Categoria atualizada com sucesso!")
        return super().form_valid(form)

# Aproveite e adicione a de Deletar também para não dar erro depois
class CategoriaDeleteView(LoginRequiredMixin, DeleteView):
    model = CategoriaFinanceira
    success_url = reverse_lazy('categoria_list')

class FornecedorCreateView(LoginRequiredMixin, CreateView):
    model = Fornecedor
    form_class = FornecedorForm
    template_name = 'financeiro_fit/fornecedor_form.html' # Nome do template do formulário
    success_url = reverse_lazy('fornecedor_list')

    def form_valid(self, form):
        messages.success(self.request, "Fornecedor cadastrado com sucesso!")
        return super().form_valid(form)

class FornecedorUpdateView(LoginRequiredMixin, UpdateView):
    model = Fornecedor
    form_class = FornecedorForm
    template_name = 'financeiro_fit/fornecedor_form.html'
    success_url = reverse_lazy('fornecedor_list')

    def form_valid(self, form):
        messages.success(self.request, "Fornecedor atualizado com sucesso!")
        return super().form_valid(form)