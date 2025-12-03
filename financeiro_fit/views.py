from django.shortcuts import render, get_object_or_404, redirect
from django.views.generic import ListView, CreateView, UpdateView, DeleteView, DetailView
from django.urls import reverse_lazy
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.decorators import login_required
from django.utils import timezone
from django.contrib import messages
from django.db.models import Sum
from django.http import HttpResponseRedirect, HttpResponse
from django.template.loader import get_template

import uuid
import csv
from dateutil.relativedelta import relativedelta
from xhtml2pdf import pisa
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment

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

def baixar_lancamento(request, pk):
    lancamento = get_object_or_404(Lancamento, pk=pk)
    
    if request.method == 'POST':
        lancamento.status = 'PAGO'
        lancamento.data_pagamento = timezone.now().date()
        lancamento.forma_pagamento = request.POST.get('forma_pagamento')
        lancamento.save()
        
        if lancamento.conta:
            if lancamento.categoria.tipo == 'RECEITA':
                lancamento.conta.saldo_atual += lancamento.valor
            else:
                lancamento.conta.saldo_atual -= lancamento.valor
            lancamento.conta.save()
        
        messages.success(request, "Baixa realizada!")
        
    next_url = request.META.get('HTTP_REFERER')
    if next_url: return HttpResponseRedirect(next_url)
    return redirect('contas_receber')

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
    template_name = 'financeiro_fit/form_generico.html'
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
        
        inicio = self.request.GET.get('inicio')
        fim = self.request.GET.get('fim')
        
        lancamentos = Lancamento.objects.filter(
            conta=self.object, 
            status='PAGO'
        ).order_by('-data_pagamento')
        
        if inicio and fim:
            lancamentos = lancamentos.filter(data_pagamento__range=[inicio, fim])
            
        context['lancamentos'] = lancamentos
        
        entradas = lancamentos.filter(categoria__tipo='RECEITA').aggregate(Sum('valor'))['valor__sum'] or 0
        saidas = lancamentos.filter(categoria__tipo='DESPESA').aggregate(Sum('valor'))['valor__sum'] or 0
        
        context['total_entradas'] = entradas
        context['total_saidas'] = saidas
        context['resultado_periodo'] = entradas - saidas
        
        return context

# ==============================================================================
# 5. EXPORTAÇÃO (EXCEL E PDF)
# ==============================================================================
    
@login_required
def exportar_extrato_excel(request, pk):
    conta = get_object_or_404(ContaBancaria, pk=pk)
    inicio = request.GET.get('inicio')
    fim = request.GET.get('fim')
    
    lancamentos = Lancamento.objects.filter(conta=conta, status='PAGO').order_by('-data_pagamento')
    if inicio and fim:
        lancamentos = lancamentos.filter(data_pagamento__range=[inicio, fim])

    # Cria o arquivo Excel
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = f"Extrato {conta.nome}"

    # Estilos
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
    money_format = 'R$ #,##0.00'
    
    # Cabeçalho
    headers = ["Data Pgto", "Descrição", "Categoria", "Tipo", "Valor"]
    ws.append(headers)
    
    for cell in ws[1]:
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal='center')

    # Dados
    for l in lancamentos:
        valor = float(l.valor)
        if l.categoria.tipo == 'DESPESA':
            valor = valor * -1
            
        row = [l.data_pagamento, l.descricao, l.categoria.nome, l.categoria.get_tipo_display(), valor]
        ws.append(row)
        
        last_row = ws.max_row
        cell_valor = ws.cell(row=last_row, column=5)
        cell_valor.number_format = money_format
        if valor < 0:
            cell_valor.font = Font(color="FF0000")
        else:
            cell_valor.font = Font(color="006100")

    ws.column_dimensions['A'].width = 15
    ws.column_dimensions['B'].width = 40
    
    response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = f'attachment; filename="Extrato_{conta.nome}.xlsx"'
    wb.save(response)
    return response

@login_required
def exportar_extrato_pdf(request, pk):
    conta = get_object_or_404(ContaBancaria, pk=pk)
    inicio = request.GET.get('inicio')
    fim = request.GET.get('fim')
    
    lancamentos = Lancamento.objects.filter(conta=conta, status='PAGO').order_by('-data_pagamento')
    if inicio and fim:
        lancamentos = lancamentos.filter(data_pagamento__range=[inicio, fim])

    template_path = 'financeiro_fit/extrato_pdf.html'
    context = {'conta': conta, 'lancamentos': lancamentos, 'inicio': inicio, 'fim': fim}
    
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