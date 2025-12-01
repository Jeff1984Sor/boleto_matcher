from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.contrib import messages
from django.template import Template, Context
from django.http import HttpResponse
from django.utils import timezone
from django.urls import reverse_lazy
from django.views.generic import ListView, CreateView, UpdateView, DeleteView
from django.contrib.auth.mixins import LoginRequiredMixin
import json

# Imports Locais
from cadastros_fit.models import Aluno
from .models import Contrato, TemplateContrato, Plano
from .forms import ContratoForm, HorarioFixoFormSet, PlanoForm
from .services import processar_novo_contrato

@login_required
def novo_contrato(request, aluno_id):
    aluno = get_object_or_404(Aluno, pk=aluno_id)
    
    # --- PREPARA DADOS DOS PLANOS PARA O JAVASCRIPT ---
    # Isso permite o preenchimento automático de valor e parcelas
    planos_data = {}
    for p in Plano.objects.filter(ativo=True):
        planos_data[p.id] = {
            'valor': float(p.valor_mensal),
            'meses': p.duracao_meses,
            'frequencia': p.frequencia_semanal
        }
    planos_json = json.dumps(planos_data)

    if request.method == 'POST':
        form = ContratoForm(request.POST)
        
        # Validamos o contrato
        if form.is_valid():
            # Cria instância na memória (sem salvar) para passar ao formset
            contrato = form.save(commit=False)
            contrato.aluno = aluno
            
            # Passa a instância com o plano selecionado para validar a frequência
            formset = HorarioFixoFormSet(request.POST, instance=contrato)
            
            if formset.is_valid():
                try:
                    with transaction.atomic():
                        contrato.save() # Salva Contrato
                        formset.save()  # Salva Horários
                        
                        # Roda a Automação (Gera Aulas e Financeiro)
                        processar_novo_contrato(contrato)
                    
                    messages.success(request, "Contrato criado com sucesso!")
                    return redirect('aluno_detail', pk=aluno.pk)
                
                except Exception as e:
                    messages.error(request, f"Erro ao processar contrato: {e}")
            else:
                messages.error(request, "Erro nos horários. Verifique se a quantidade condiz com o Plano.")
        else:
            messages.error(request, "Verifique os dados do formulário.")
            # Recria o formset vazio se falhar, para não quebrar o HTML
            formset = HorarioFixoFormSet(request.POST)

    else:
        # GET: Formulário vazio
        form = ContratoForm() 
        formset = HorarioFixoFormSet()

    return render(request, 'contratos_fit/novo_contrato.html', {
        'form': form,
        'formset': formset,
        'aluno': aluno,
        'planos_json': planos_json # <--- Envia dados para o JS
    })

@login_required
def imprimir_contrato(request, pk):
    contrato = get_object_or_404(Contrato, pk=pk)
    
    # Busca template
    template_obj = contrato.template_usado
    if not template_obj:
        template_obj = TemplateContrato.objects.filter(ativo=True).first()
        
    if not template_obj:
        return HttpResponse("<h1>Erro:</h1> <p>Nenhum 'Template de Contrato' cadastrado no sistema. Vá ao Admin e crie um.</p>")

    # Contexto para substituir as variáveis {{ aluno.nome }}
    contexto_dados = Context({
        'aluno': contrato.aluno,
        'contrato': contrato,
        'plano': contrato.plano,
        'unidade': contrato.unidade,
        'hoje': timezone.now().date(),
        'empresa_nome': "MayaCorp Fit",
    })

    try:
        template_django = Template(template_obj.texto_html)
        conteudo_final = template_django.render(contexto_dados)
    except Exception as e:
        return HttpResponse(f"Erro ao gerar contrato: {e}")

    return render(request, 'contratos_fit/print_layout.html', {
        'conteudo': conteudo_final,
        'titulo': f"Contrato - {contrato.aluno.nome}"
    })

# --- CRUD DE PLANOS ---

class PlanoListView(LoginRequiredMixin, ListView):
    model = Plano
    template_name = 'contratos_fit/plano_list.html'
    context_object_name = 'planos'

class PlanoCreateView(LoginRequiredMixin, CreateView):
    model = Plano
    form_class = PlanoForm
    template_name = 'contratos_fit/plano_form.html'
    success_url = reverse_lazy('plano_list')

class PlanoUpdateView(LoginRequiredMixin, UpdateView):
    model = Plano
    form_class = PlanoForm
    template_name = 'contratos_fit/plano_form.html'
    success_url = reverse_lazy('plano_list')

class PlanoDeleteView(LoginRequiredMixin, DeleteView):
    model = Plano
    template_name = 'contratos_fit/plano_confirm_delete.html'
    success_url = reverse_lazy('plano_list')

@login_required
def lista_contratos_aluno(request, aluno_id):
    aluno = get_object_or_404(Aluno, pk=aluno_id)
    contratos = aluno.contratos.all().order_by('-data_inicio')
    
    return render(request, 'contratos_fit/aluno_contratos_list.html', {
        'aluno': aluno,
        'contratos': contratos,
        'hoje': timezone.now().date()
    })