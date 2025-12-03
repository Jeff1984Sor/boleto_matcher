from django.shortcuts import render, get_object_or_404, redirect
from django.views.generic import ListView, CreateView, UpdateView, DeleteView, DetailView
from django.urls import reverse_lazy
from django.contrib.auth.mixins import LoginRequiredMixin
from django.utils import timezone 
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from financeiro_fit.models import Lancamento

# Imports Locais
from .models import Aluno, Profissional, Unidade
from .forms import AlunoForm, ProfissionalForm, UnidadeForm, DocumentoExtraForm
from .services import OCRService
from agenda_fit.models import Presenca, Aula 

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone
from datetime import timedelta

API_KEY_N8N = "segredo_mayacorp_n8n_123" 

# --- ALUNOS ---
class AlunoListView(LoginRequiredMixin, ListView):
    model = Aluno
    template_name = 'cadastros_fit/aluno_list.html'
    context_object_name = 'alunos'

class AlunoCreateView(LoginRequiredMixin, CreateView):
    model = Aluno
    form_class = AlunoForm
    template_name = 'cadastros_fit/aluno_form.html'
    success_url = reverse_lazy('aluno_list')
    # REMOVIDO: form_valid manual (não precisa mais injetar organização)

class AlunoUpdateView(LoginRequiredMixin, UpdateView):
    model = Aluno
    form_class = AlunoForm
    template_name = 'cadastros_fit/aluno_form.html'
    success_url = reverse_lazy('aluno_list')

class AlunoDeleteView(LoginRequiredMixin, DeleteView):
    model = Aluno
    template_name = 'cadastros_fit/aluno_confirm_delete.html'
    success_url = reverse_lazy('aluno_list')

class AlunoDetailView(LoginRequiredMixin, DetailView):
    model = Aluno
    template_name = 'cadastros_fit/aluno_detail.html'
    context_object_name = 'aluno'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        aluno = self.object
        agora = timezone.now()

        # Resumo (Visão Geral)
        proxima_presenca = Presenca.objects.filter(
            aluno=aluno,
            aula__data_hora_inicio__gte=agora,
            aula__status__in=['AGENDADA', 'CONFIRMADA']
        ).select_related('aula', 'aula__profissional').order_by('aula__data_hora_inicio').first()
        context['proxima_aula'] = proxima_presenca.aula if proxima_presenca else None

        ultima_evolucao = Aula.objects.filter(
            presencas__aluno=aluno,
            data_hora_inicio__lt=agora,
        ).exclude(evolucao_texto='').order_by('-data_hora_inicio').first()
        context['ultima_evolucao'] = ultima_evolucao

        # Histórico Completo (Para as abas)
        context['agenda_completa'] = Presenca.objects.filter(aluno=aluno).select_related('aula', 'aula__profissional').order_by('-aula__data_hora_inicio')
        context['financeiro_completo'] = Lancamento.objects.filter(aluno=aluno).select_related('categoria').order_by('data_vencimento')
        context['contratos_completo'] = aluno.contratos.all().order_by('-criado_em')
        context['documentos_extras'] = aluno.documentos.all()
        
        return context

# --- PROFISSIONAIS ---
class ProfissionalListView(LoginRequiredMixin, ListView):
    model = Profissional
    template_name = 'cadastros_fit/profissional_list.html'
    context_object_name = 'profissionais'

class ProfissionalCreateView(LoginRequiredMixin, CreateView):
    model = Profissional
    form_class = ProfissionalForm
    template_name = 'cadastros_fit/profissional_form.html'
    success_url = reverse_lazy('profissional_list')
    # REMOVIDO: form_valid manual

class ProfissionalUpdateView(LoginRequiredMixin, UpdateView):
    model = Profissional
    form_class = ProfissionalForm
    template_name = 'cadastros_fit/profissional_form.html'
    success_url = reverse_lazy('profissional_list')

# --- UNIDADES ---
class UnidadeListView(LoginRequiredMixin, ListView):
    model = Unidade
    template_name = 'cadastros_fit/unidade_list.html'
    context_object_name = 'unidades'

class UnidadeCreateView(LoginRequiredMixin, CreateView):
    model = Unidade
    form_class = UnidadeForm
    template_name = 'cadastros_fit/unidade_form.html'
    success_url = reverse_lazy('unidade_list')
    # REMOVIDO: form_valid manual

class UnidadeUpdateView(LoginRequiredMixin, UpdateView):
    model = Unidade
    form_class = UnidadeForm
    template_name = 'cadastros_fit/unidade_form.html'
    success_url = reverse_lazy('unidade_list')

class UnidadeDeleteView(LoginRequiredMixin, DeleteView):
    model = Unidade
    template_name = 'cadastros_fit/unidade_confirm_delete.html'
    success_url = reverse_lazy('unidade_list')

# --- APIS E UTILITÁRIOS ---

@csrf_exempt 
def api_ler_documento(request):
    if request.method == 'POST' and request.FILES.get('imagem'):
        tipo = request.POST.get('tipo') 
        imagem = request.FILES['imagem']
        
        if tipo == 'identidade':
            dados = OCRService.extrair_dados_identidade(imagem)
        elif tipo == 'endereco':
            dados = OCRService.extrair_dados_endereco(imagem)
        else:
            return JsonResponse({'erro': 'Tipo inválido'}, status=400)
            
        return JsonResponse(dados)
    
    return JsonResponse({'erro': 'Envie uma imagem via POST'}, status=400)

def upload_documento_extra(request, pk):
    """Recebe o upload do Modal e salva vinculado ao Aluno (pk)"""
    aluno = get_object_or_404(Aluno, pk=pk)
    
    if request.method == 'POST':
        form = DocumentoExtraForm(request.POST, request.FILES)
        if form.is_valid():
            doc = form.save(commit=False)
            doc.aluno = aluno
            doc.save()
            
    return redirect('aluno_detail', pk=pk)

@csrf_exempt
def api_agenda_amanha(request):
    # 1. Segurança Básica (Verifica Token no Header)
    token = request.headers.get('X-API-KEY')
    if token != API_KEY_N8N:
        return JsonResponse({'erro': 'Acesso negado'}, status=403)

    # 2. Calcula data de amanhã
    amanha = timezone.now().date() + timedelta(days=1)
    
    # 3. Busca aulas de amanhã (que não foram canceladas)
    aulas = Aula.objects.filter(
        data_hora_inicio__date=amanha
    ).exclude(status='CANCELADA').select_related('profissional').prefetch_related('presencas__aluno')

    # 4. Agrupa por Profissional
    dados_envio = {}

    for aula in aulas:
        prof = aula.profissional
        
        # Se o prof não tem email, pula
        if not prof.email:
            continue
            
        # Se é a primeira vez que vemos esse prof no loop, cria a estrutura
        if prof.id not in dados_envio:
            dados_envio[prof.id] = {
                "profissional": prof.nome,
                "email": prof.email,
                "data": amanha.strftime('%d/%m/%Y'),
                "aulas": []
            }
        
        # Lista os alunos dessa aula
        alunos_lista = [p.aluno.nome for p in aula.presencas.all()]
        if not alunos_lista:
            alunos_lista = ["Nenhum aluno agendado (Vaga livre)"]

        # Adiciona a aula na lista do prof
        dados_envio[prof.id]["aulas"].append({
            "horario": aula.data_hora_inicio.strftime('%H:%M'),
            "alunos": ", ".join(alunos_lista)
        })

    # 5. Retorna a lista pronta para o n8n (converte dict values para lista)
    return JsonResponse(list(dados_envio.values()), safe=False)