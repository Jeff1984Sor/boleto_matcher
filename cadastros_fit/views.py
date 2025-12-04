from django.shortcuts import render, get_object_or_404, redirect
from django.views.generic import ListView, CreateView, UpdateView, DeleteView, DetailView
from django.urls import reverse_lazy
from django.contrib.auth.mixins import LoginRequiredMixin
from django.utils import timezone 
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

# Imports Locais
from .models import Aluno, Profissional, Unidade
from .forms import AlunoForm, ProfissionalForm, UnidadeForm, DocumentoExtraForm
from .services import OCRService

# Imports de Outros Apps
from agenda_fit.models import Presenca, Aula 
from financeiro_fit.models import Lancamento
from contratos_fit.models import Contrato

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
    # O form_valid manual não é mais necessário (Model sem organização)

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
        context['agora'] = agora

        # --- ABA 1: VISÃO GERAL (Resumos) ---
        context['proxima_aula'] = Aula.objects.filter(
            presencas__aluno=aluno,
            data_hora_inicio__gte=agora,
            status='AGENDADA'
        ).order_by('data_hora_inicio').first()

        context['ultima_evolucao'] = Aula.objects.filter(
            presencas__aluno=aluno,
            data_hora_inicio__lt=agora,
        ).exclude(evolucao_texto='').order_by('-data_hora_inicio').first()

        # --- ABA 2: AGENDA COMPLETA ---
        context['agenda_completa'] = Presenca.objects.filter(
            aluno=aluno
        ).select_related('aula', 'aula__profissional').order_by('-aula__data_hora_inicio')

        # --- ABA 3: FINANCEIRO COMPLETO ---
        context['financeiro_completo'] = Lancamento.objects.filter(
            aluno=aluno
        ).select_related('categoria').order_by('data_vencimento')

        # --- ABA 4: CONTRATOS ---
        context['contratos_completo'] = Contrato.objects.filter(
            aluno=aluno
        ).select_related('plano').order_by('-created_at')

        # Docs Extras
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