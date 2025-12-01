from django.shortcuts import render

# Create your views here.
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.contrib import messages

# Imports dos Models e Forms
from cadastros_fit.models import Aluno
from .forms import ContratoForm, HorarioFixoFormSet
from .services import processar_novo_contrato
from django.template import Template, Context
from django.http import HttpResponse
from .models import Contrato, TemplateContrato 

from django.utils import timezone

from django.urls import reverse_lazy
from .models import Plano
from .forms import PlanoForm

from django.views.generic import ListView, CreateView, UpdateView, DeleteView
from django.contrib.auth.mixins import LoginRequiredMixin

import datetime 

@login_required
def novo_contrato(request, aluno_id):
    # Busca o aluno ou retorna 404 se não existir
    aluno = get_object_or_404(Aluno, pk=aluno_id)
    
    if request.method == 'POST':
        form = ContratoForm(request.POST)
        
        # Precisamos criar a instância do contrato na memória (sem salvar no banco ainda)
        # para passar para o Formset validar a frequência do plano.
        if form.is_valid():
            contrato = form.save(commit=False)
            contrato.aluno = aluno  # Vincula o contrato ao aluno da URL
            
            # Passa a instância 'contrato' (que já tem o plano selecionado) para o Formset
            formset = HorarioFixoFormSet(request.POST, instance=contrato)
            
            if formset.is_valid():
                try:
                    # 'atomic' garante que ou salva tudo (contrato + horários + automação) ou não salva nada
                    with transaction.atomic():
                        # 1. Salva o Contrato no Banco
                        contrato.save()
                        
                        # 2. Salva os Horários Fixos
                        formset.save()
                        
                        # 3. Roda a Automação (Cria Aulas na Agenda e Contas no Financeiro)
                        processar_novo_contrato(contrato)
                    
                    messages.success(request, f"Sucesso! Contrato criado, agenda gerada e financeiro lançado.")
                    return redirect('aluno_detail', pk=aluno.pk) # Volta para a ficha do aluno
                
                except Exception as e:
                    # Se der erro na automação, o 'atomic' desfaz o salvamento do contrato
                    messages.error(request, f"Erro ao processar contrato: {e}")
            else:
                messages.error(request, "Erro nos horários. Verifique se a quantidade condiz com o Plano.")
        else:
            messages.error(request, "Verifique os dados do formulário.")
            # Recria o formset vazio para não quebrar o template caso o form principal esteja inválido
            formset = HorarioFixoFormSet(request.POST)

    else:
        # GET: Formulário vazio
        # REMOVIDO: initial={'unidade': aluno.unidade} pois aluno não tem unidade fixa mais
        form = ContratoForm() 
        formset = HorarioFixoFormSet()

    return render(request, 'contratos_fit/novo_contrato.html', {
        'form': form,
        'formset': formset,
        'aluno': aluno
    })

@login_required
def imprimir_contrato(request, pk):
    contrato = get_object_or_404(Contrato, pk=pk)
    
    # 1. Tenta achar o template usado no contrato ou pega o primeiro ativo
    template_obj = contrato.template_usado
    if not template_obj:
        template_obj = TemplateContrato.objects.filter(ativo=True).first()
        
    if not template_obj:
        return HttpResponse("<h1>Erro:</h1> <p>Nenhum 'Template de Contrato' cadastrado no sistema. Vá ao Admin e crie um.</p>")

    # 2. Prepara os dados disponíveis para usar no texto
    # Tudo que você colocar aqui fica disponível com {{ }} no HTML do banco
    contexto_dados = Context({
        'aluno': contrato.aluno,
        'contrato': contrato,
        'plano': contrato.plano,
        'unidade': contrato.unidade,
        'hoje': timezone.now().date(),
        'empresa_nome': "MayaCorp Fit",
    })

    # 3. Renderiza o texto do banco (Transforma {{aluno.nome}} em "João")
    try:
        template_django = Template(template_obj.texto_html)
        conteudo_final = template_django.render(contexto_dados)
    except Exception as e:
        return HttpResponse(f"Erro ao gerar contrato: {e}")

    # 4. Entrega para o HTML de impressão
    return render(request, 'contratos_fit/print_layout.html', {
        'conteudo': conteudo_final,
        'titulo': f"Contrato - {contrato.aluno.nome}"
    })

# --- PLANOS ---

class PlanoListView(LoginRequiredMixin, ListView):
    model = Plano
    template_name = 'contratos_fit/plano_list.html'
    context_object_name = 'planos'

class PlanoCreateView(LoginRequiredMixin, CreateView):
    model = Plano
    form_class = PlanoForm
    template_name = 'contratos_fit/plano_form.html'
    success_url = reverse_lazy('plano_list')

    # Se você removeu o campo organizacao do Model, pode apagar este método.
    # Se o campo ainda existe no banco/model, mantenha isso para evitar erro:
    # def form_valid(self, form):
    #     form.instance.organizacao = self.request.tenant
    #     return super().form_valid(form)

class PlanoUpdateView(LoginRequiredMixin, UpdateView):
    model = Plano
    form_class = PlanoForm
    template_name = 'contratos_fit/plano_form.html'
    success_url = reverse_lazy('plano_list')

class PlanoDeleteView(LoginRequiredMixin, DeleteView):
    model = Plano
    template_name = 'contratos_fit/plano_confirm_delete.html'
    success_url = reverse_lazy('plano_list')