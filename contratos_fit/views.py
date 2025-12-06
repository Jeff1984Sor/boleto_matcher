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
import locale
from datetime import datetime
from financeiro_fit.models import Lancamento
from agenda_fit.models import Presenca
import json
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.shortcuts import render, get_object_or_404, redirect
from django.utils import timezone
from django.http import HttpResponse, JsonResponse
from django.template import Template, Context
from django.views.decorators.csrf import csrf_exempt
import base64
from django.core.files.base import ContentFile

from .models import Contrato

import base64
from django.utils import timezone
from django.http import JsonResponse
from django.template import Template, Context
from django.views.decorators.csrf import csrf_exempt
from django.urls import reverse
import json
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.contrib import messages
from .models import Plano, Contrato
from cadastros_fit.models import Aluno
from .forms import ContratoForm, HorarioFixoFormSet
from .services import processar_novo_contrato
from .services import processar_novo_contrato, disparar_email_contrato 

# Tenta importar a função do N8N (se não existir, não quebra)
try:
    from .services import enviar_contrato_n8n
except ImportError:
    enviar_contrato_n8n = None


# Imports Locais
from cadastros_fit.models import Aluno
from .models import Contrato, TemplateContrato, Plano
from .forms import ContratoForm, HorarioFixoFormSet, PlanoForm
from .services import processar_novo_contrato, regenerar_contrato

# Tenta configurar local para datas em Português (pode depender do sistema operacional do servidor)
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except:
    pass # Se der erro no servidor linux/windows, segue o padrão

# ==============================================================================
# LISTA MESTRA DE VARIÁVEIS (Para usar no Editor e na Impressão)
# ==============================================================================
def get_variaveis_contrato():
    """Retorna a lista completa de variáveis disponíveis para o editor"""
    return [
        # --- ALUNO ---
        {'cat': 'Aluno', 'codigo': '{{ aluno.nome }}', 'desc': 'Nome Completo'},
        {'cat': 'Aluno', 'codigo': '{{ aluno.cpf }}', 'desc': 'CPF'},
        {'cat': 'Aluno', 'codigo': '{{ aluno.rg }}', 'desc': 'RG (Se houver)'},
        {'cat': 'Aluno', 'codigo': '{{ aluno.data_nascimento }}', 'desc': 'Data de Nascimento (dd/mm/aaaa)'},
        {'cat': 'Aluno', 'codigo': '{{ aluno.telefone }}', 'desc': 'Telefone / Celular'},
        {'cat': 'Aluno', 'codigo': '{{ aluno.email }}', 'desc': 'E-mail'},
        {'cat': 'Aluno', 'codigo': '{{ aluno.endereco_completo }}', 'desc': 'Endereço Completo (Rua, Nº, Bairro...)'},
        {'cat': 'Aluno', 'codigo': '{{ aluno.cep }}', 'desc': 'CEP'},
        {'cat': 'Aluno', 'codigo': '{{ aluno.cidade }}', 'desc': 'Cidade'},
        {'cat': 'Aluno', 'codigo': '{{ aluno.estado }}', 'desc': 'Estado (UF)'},

        # --- CONTRATO ---
        {'cat': 'Contrato', 'codigo': '{{ contrato.id }}', 'desc': 'Número do Contrato'},
        {'cat': 'Contrato', 'codigo': '{{ contrato.data_inicio }}', 'desc': 'Data Início (dd/mm/aaaa)'},
        {'cat': 'Contrato', 'codigo': '{{ contrato.data_fim }}', 'desc': 'Data Fim (dd/mm/aaaa)'},
        {'cat': 'Contrato', 'codigo': '{{ contrato.dia_vencimento }}', 'desc': 'Dia do Vencimento'},
        {'cat': 'Contrato', 'codigo': '{{ contrato.criado_em }}', 'desc': 'Data de Criação do Registro'},

        # --- FINANCEIRO ---
        {'cat': 'Financeiro', 'codigo': '{{ contrato.valor_total }}', 'desc': 'Valor Total do Contrato (R$)'},
        {'cat': 'Financeiro', 'codigo': '{{ contrato.qtde_parcelas }}', 'desc': 'Quantidade de Parcelas'},
        {'cat': 'Financeiro', 'codigo': '{{ valor_parcela }}', 'desc': 'Valor da Parcela Mensal (R$)'},
        {'cat': 'Financeiro', 'codigo': '{{ valor_extenso }}', 'desc': 'Valor Total por Extenso'},

        # --- PLANO / SERVIÇO ---
        {'cat': 'Plano', 'codigo': '{{ plano.nome }}', 'desc': 'Nome do Plano'},
        {'cat': 'Plano', 'codigo': '{{ plano.frequencia_semanal }}', 'desc': 'Vezes por Semana'},
        {'cat': 'Plano', 'codigo': '{{ plano.duracao_meses }}', 'desc': 'Duração em Meses'},

        # --- EMPRESA / UNIDADE ---
        {'cat': 'Empresa', 'codigo': '{{ empresa_nome }}', 'desc': 'Nome da Empresa (Tenant)'},
        {'cat': 'Empresa', 'codigo': '{{ unidade.nome }}', 'desc': 'Nome da Unidade'},
        {'cat': 'Empresa', 'codigo': '{{ unidade.endereco }}', 'desc': 'Endereço da Unidade'},
        {'cat': 'Empresa', 'codigo': '{{ unidade.telefone }}', 'desc': 'Telefone da Unidade'},

        # --- DATAS ---
        {'cat': 'Datas', 'codigo': '{{ hoje }}', 'desc': 'Data de Hoje (dd/mm/aaaa)'},
        {'cat': 'Datas', 'codigo': '{{ hoje_extenso }}', 'desc': 'Data por Extenso (Ex: 01 de Janeiro de 2025)'},
        {'cat': 'Datas', 'codigo': '{{ ano_atual }}', 'desc': 'Ano Atual (Ex: 2025)'},
    ]

# ==============================================================================
# 1. IMPRESSÃO (Onde a mágica acontece)
# ==============================================================================
@login_required
def imprimir_contrato(request, pk):
    contrato = get_object_or_404(Contrato, pk=pk)
    
    template_obj = contrato.template_usado
    if not template_obj:
        template_obj = TemplateContrato.objects.filter(ativo=True).first()
        
    if not template_obj:
        return HttpResponse("<h1>Erro:</h1> <p>Nenhum modelo de contrato cadastrado.</p>")

    # Cálculos Extras para o Template
    valor_parcela = 0
    if contrato.qtde_parcelas > 0:
        valor_parcela = contrato.valor_total / contrato.qtde_parcelas
    
    hoje = timezone.now().date()
    
    # Formatação por extenso (gambiarra simples caso o locale falhe no servidor)
    meses = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
    hoje_extenso = f"{hoje.day} de {meses[hoje.month-1]} de {hoje.year}"

    # Monta o dicionário de dados
    contexto_dados = Context({
        # Objetos
        'aluno': contrato.aluno,
        'contrato': contrato,
        'plano': contrato.plano,
        'unidade': contrato.unidade,
        
        # Empresa (Tenant)
        'empresa_nome': request.tenant.nome if hasattr(request.tenant, 'nome') else "MayaCorp Fit",
        
        # Calculados
        'valor_parcela': f"{valor_parcela:.2f}".replace('.', ','),
        'valor_total': f"{contrato.valor_total:.2f}".replace('.', ','), # Sobrescreve para formatar pt-BR
        'valor_extenso': "Valor por extenso indisponível (instalar num2words)", # Opcional: instalar lib num2words
        
        # Datas
        'hoje': hoje.strftime('%d/%m/%Y'),
        'hoje_extenso': hoje_extenso,
        'ano_atual': hoje.year,
    })

    try:
        template_django = Template(template_obj.texto_html)
        conteudo_final = template_django.render(contexto_dados)
    except Exception as e:
        return HttpResponse(f"Erro ao processar variáveis do contrato: {e}")

    return render(request, 'contratos_fit/print_layout.html', {
        'conteudo': conteudo_final,
        'titulo': f"Contrato - {contrato.aluno.nome}"
    })

# ==============================================================================
# 2. VENDA DE CONTRATO
# ==============================================================================

from .models import Contrato, HorarioFixo, Plano, TemplateContrato 

@login_required
def novo_contrato(request, aluno_id):
    aluno = get_object_or_404(Aluno, pk=aluno_id)
    
    # Prepara dados para o JavaScript (cálculo de preço dinâmico)
    planos_data = {
        p.id: {
            'valor': float(p.valor_total_sugerido),
            'freq': p.frequencia_semanal,
            'meses': p.duracao_meses
        } 
        for p in Plano.objects.all()
    }

    if request.method == 'POST':
        form = ContratoForm(request.POST)
        
        # Cria a instância em memória para validação do formset
        contrato_instance = form.save(commit=False) if form.is_valid() else None
        formset = HorarioFixoFormSet(request.POST, instance=contrato_instance)
        
        if form.is_valid() and formset.is_valid():
            try:
                with transaction.atomic():
                    # 1. Configura o Contrato
                    contrato = form.save(commit=False)
                    contrato.aluno = aluno
                    
                    # --- CORREÇÃO: Vincula o Template Padrão automaticamente ---
                    if not contrato.template_usado:
                        template_padrao = TemplateContrato.objects.filter(ativo=True).first()
                        if template_padrao:
                            contrato.template_usado = template_padrao
                    # -----------------------------------------------------------

                    contrato.save()
                    
                    # 2. Salva Horários
                    formset.instance = contrato
                    formset.save()
                    
                    # 3. Gera Agenda e Financeiro
                    processar_novo_contrato(contrato)
                    
                    # 4. Envia E-mail de Assinatura
                    if contrato.aluno.email:
                        sucesso, msg = disparar_email_contrato(contrato, request.get_host())
                        if sucesso:
                            messages.success(request, f"Contrato criado e {msg}")
                        else:
                            messages.warning(request, f"Contrato criado, mas falha no e-mail: {msg}")
                    else:
                        messages.warning(request, "Contrato criado. E-mail não enviado (Aluno sem e-mail).")

                    return redirect('aluno_detail', pk=aluno.id)
            except Exception as e:
                messages.error(request, f"Erro ao processar: {e}")
                print(f"ERRO CRÍTICO: {e}")
    else:
        form = ContratoForm()
        formset = HorarioFixoFormSet()

    return render(request, 'contratos_fit/novo_contrato.html', {
        'form': form,
        'formset': formset,
        'aluno': aluno,
        'planos_json': json.dumps(planos_data)
    })

@login_required
def lista_contratos_aluno(request, aluno_id):
    aluno = get_object_or_404(Aluno, pk=aluno_id)
    contratos = aluno.contratos.all().order_by('-data_inicio')
    return render(request, 'contratos_fit/lista_contratos_aluno.html', {
        'aluno': aluno, 'contratos': contratos, 'hoje': timezone.now().date()
    })

class ContratoListView(LoginRequiredMixin, ListView):
    model = Contrato
    template_name = 'contratos_fit/contrato_list.html'
    context_object_name = 'contratos'
    ordering = ['-criado_em']
    paginate_by = 20

    def get_queryset(self):
        qs = super().get_queryset()
        aluno = self.request.GET.get('aluno')
        if aluno:
            qs = qs.filter(aluno__nome__icontains=aluno)
        return qs

class ContratoUpdateView(LoginRequiredMixin, UpdateView):
    model = Contrato
    form_class = ContratoForm
    template_name = 'contratos_fit/novo_contrato.html'
    success_url = reverse_lazy('contrato_list')

    def form_valid(self, form):
        response = super().form_valid(form)
        regenerar_contrato(self.object)
        return response

class ContratoDeleteView(LoginRequiredMixin, DeleteView):
    model = Contrato
    template_name = 'contratos_fit/contrato_confirm_delete.html'
    
    def get_success_url(self):
        # Redireciona para o aluno dono do contrato
        return reverse_lazy('aluno_detail', kwargs={'pk': self.object.aluno.pk})

    def form_valid(self, form):
        contrato = self.get_object()
        
        # LOGICA SEGURA: Excluir parcelas EM ABERTO deste contrato
        parcelas_pendentes = Lancamento.objects.filter(contrato=contrato, status='PENDENTE')
        qtd_financeiro = parcelas_pendentes.count()
        parcelas_pendentes.delete()
        
        # Opcional: Limpar aulas futuras agendadas (Descomente se quiser)
        # Presenca.objects.filter(aluno=contrato.aluno, aula__data_hora_inicio__gte=timezone.now(), status='AGENDADA').delete()

        messages.warning(self.request, f"Contrato excluído! {qtd_financeiro} parcelas em aberto foram removidas.")
        return super().form_valid(form)

@login_required
def encerrar_contrato(request, pk):
    contrato = get_object_or_404(Contrato, pk=pk)
    
    if request.method == 'POST':
        # Marca como cancelado/encerrado
        contrato.status = 'CANCELADO'
        contrato.save()
        
        # Remove cobranças futuras que ainda não foram pagas
        parcelas = Lancamento.objects.filter(contrato=contrato, status='PENDENTE')
        parcelas.delete()
        
        messages.success(request, "Contrato encerrado e cobranças futuras canceladas.")
        return redirect('aluno_detail', pk=contrato.aluno.pk)
    
    # Se tentar acessar via GET, manda para uma tela de confirmação ou volta
    return redirect('aluno_detail', pk=contrato.aluno.pk)

# ==============================================================================
# 3. PLANOS
# ==============================================================================

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

# ==============================================================================
# 4. TEMPLATES (MODELOS)
# ==============================================================================

class TemplateListView(LoginRequiredMixin, ListView):
    model = TemplateContrato
    template_name = 'contratos_fit/template_list.html'
    context_object_name = 'templates'

class TemplateCreateView(LoginRequiredMixin, CreateView):
    model = TemplateContrato
    fields = ['nome', 'texto_html', 'ativo']
    template_name = 'contratos_fit/template_editor.html'
    success_url = reverse_lazy('template_list')

    def form_valid(self, form):
        form.instance.organizacao = self.request.tenant
        return super().form_valid(form)
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['variaveis'] = get_variaveis_contrato() # <--- Pega a lista completa
        return context

class TemplateEditorView(LoginRequiredMixin, UpdateView):
    model = TemplateContrato
    fields = ['nome', 'texto_html', 'ativo']
    template_name = 'contratos_fit/template_editor.html'
    success_url = reverse_lazy('template_list')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['variaveis'] = get_variaveis_contrato() # <--- Pega a lista completa
        return context
    

def assinar_contrato_view(request, token):
    """
    Tela pública onde o aluno (ou recepção) vê o contrato e assina.
    """
    contrato = get_object_or_404(Contrato, token_assinatura=token)
    
    # Se já assinou, avisa e bloqueia
    if contrato.status in ['ASSINADO_DIGITAL', 'ASSINADO_STUDIO', 'ASSINADO_PAPEL']:
        return render(request, 'contratos_fit/ja_assinado.html', {'contrato': contrato})

    # Renderiza o texto do contrato (igual fizemos na impressão)
    if contrato.template_usado:
        t = Template(contrato.template_usado.texto_html)
        c = Context({'aluno': contrato.aluno, 'contrato': contrato, 'plano': contrato.plano, 'empresa_nome': "MayaFit"})
        texto_renderizado = t.render(c)
    else:
        texto_renderizado = "<p>Erro: Template não encontrado.</p>"

    if request.method == 'POST':
        # Recebe a imagem da assinatura (base64) vinda do JS
        assinatura_b64 = request.POST.get('assinatura_data') # String longa data:image/png...
        origem = request.POST.get('origem') # 'EMAIL' ou 'STUDIO'
        
        if assinatura_b64:
            contrato.assinatura_imagem = assinatura_b64
            contrato.data_assinatura = timezone.now()
            contrato.ip_assinatura = get_client_ip(request) # Função auxiliar
            
            if origem == 'STUDIO':
                contrato.status = 'ASSINADO_STUDIO'
            else:
                contrato.status = 'ASSINADO_DIGITAL'
            
            contrato.save()
            
            # TODO: Aqui chamaremos a função para Gerar o PDF Final e salvar em arquivo_assinado
            # gerar_pdf_final(contrato) 

            return JsonResponse({'status': 'ok', 'msg': 'Assinado com sucesso!'})
            
    return render(request, 'contratos_fit/assinar.html', {
        'contrato': contrato,
        'texto_contrato': texto_renderizado
    })

def get_client_ip(request):
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip

def get_client_ip(request):
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        return x_forwarded_for.split(',')[0]
    return request.META.get('REMOTE_ADDR')

def assinar_contrato_view(request, token):
    # Busca o contrato pelo Token secreto (sem precisar de login)
    contrato = get_object_or_404(Contrato, token_assinatura=token)
    
    # Se já assinou, bloqueia
    if contrato.status in ['ASSINADO_DIGITAL', 'ASSINADO_PRESENCIAL', 'ASSINADO_PAPEL']:
        return render(request, 'contratos_fit/ja_assinado.html', {'contrato': contrato})

    # Renderiza o texto do contrato (Substitui {{aluno.nome}} pelos dados reais)
    texto_renderizado = ""
    if contrato.template_usado:
        t = Template(contrato.template_usado.texto_html)
        c = Context({
            'aluno': contrato.aluno, 
            'contrato': contrato, 
            'plano': contrato.plano, 
            'empresa_nome': "Studio MayaCorp"
        })
        texto_renderizado = t.render(c)

    # Processa a assinatura (POST via AJAX)
    if request.method == 'POST':
        assinatura_b64 = request.POST.get('assinatura_data') # Imagem Base64
        origem = request.POST.get('origem') # STUDIO ou EMAIL
        
        if assinatura_b64:
            contrato.assinatura_imagem = assinatura_b64
            contrato.data_assinatura = timezone.now()
            contrato.ip_assinatura = get_client_ip(request)
            
            if origem == 'STUDIO':
                contrato.status = 'ASSINADO_PRESENCIAL'
            else:
                contrato.status = 'ASSINADO_DIGITAL'
            
            contrato.save()
            return JsonResponse({'status': 'ok'})

    return render(request, 'contratos_fit/assinar.html', {
        'contrato': contrato,
        'texto_contrato': texto_renderizado
    })


def enviar_contrato_email(request, pk):
    contrato = get_object_or_404(Contrato, pk=pk)
    aluno = contrato.aluno
    
    if not aluno.email:
        return HttpResponse("Erro: O aluno não tem e-mail cadastrado.")

    # Gera o link absoluto (com https://dominio...)
    link_assinatura = request.build_absolute_uri(
        reverse('assinar_contrato', args=[contrato.token_assinatura])
    )

    assunto = f"Assinatura de Contrato - {contrato.plano.nome}"
    mensagem = f"""
    Olá, {aluno.nome}!
    
    Seu contrato do plano {contrato.plano.nome} já está disponível para assinatura.
    
    Clique no link abaixo para ler e assinar digitalmente:
    {link_assinatura}
    
    Atenciosamente,
    Equipe MayaCorp Fit
    """

    try:
        send_mail(
            subject=assunto,
            message=mensagem,
            from_email=None, # Usa o DEFAULT do settings
            recipient_list=[aluno.email],
            fail_silently=False,
        )
        
        # Atualiza status
        if contrato.status == 'PENDENTE':
            contrato.status = 'ENVIADO_EMAIL'
            contrato.save()
            
        messages.success(request, f"E-mail enviado com sucesso para {aluno.email}!")
        
    except Exception as e:
        messages.error(request, f"Erro ao enviar e-mail: {e}")

    return redirect('aluno_detail', pk=aluno.id)