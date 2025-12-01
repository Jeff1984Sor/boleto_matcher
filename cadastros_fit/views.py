from django.shortcuts import render
from django.views.generic import ListView, CreateView, UpdateView, DeleteView
from django.urls import reverse_lazy
from django.contrib.auth.mixins import LoginRequiredMixin
from .models import Aluno, Profissional, Unidade
from .forms import AlunoForm, ProfissionalForm, UnidadeForm
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .services import OCRService
from django.views.generic import DetailView
from django.shortcuts import get_object_or_404, redirect
from .forms import DocumentoExtraForm

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

class AlunoUpdateView(LoginRequiredMixin, UpdateView):
    model = Aluno
    form_class = AlunoForm
    template_name = 'cadastros_fit/aluno_form.html'
    success_url = reverse_lazy('aluno_list')

class AlunoDeleteView(LoginRequiredMixin, DeleteView):
    model = Aluno
    template_name = 'cadastros_fit/aluno_confirm_delete.html'
    success_url = reverse_lazy('aluno_list')

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

@csrf_exempt # Facilitar o POST via JS por enquanto
def api_ler_documento(request):
    if request.method == 'POST' and request.FILES.get('imagem'):
        tipo = request.POST.get('tipo') # 'identidade' ou 'endereco'
        imagem = request.FILES['imagem']
        
        print(f"🤖 Iniciando leitura de {tipo} via IA...")
        
        if tipo == 'identidade':
            dados = OCRService.extrair_dados_identidade(imagem)
        elif tipo == 'endereco':
            dados = OCRService.extrair_dados_endereco(imagem)
        else:
            return JsonResponse({'erro': 'Tipo inválido'}, status=400)
            
        return JsonResponse(dados)
    
    return JsonResponse({'erro': 'Envie uma imagem via POST'}, status=400)

class AlunoDetailView(LoginRequiredMixin, DetailView):
    model = Aluno
    template_name = 'cadastros_fit/aluno_detail.html'
    context_object_name = 'aluno' # No HTML vamos usar {{ aluno.nome }}
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # 1. Documentos (que você já tinha)
        context['documentos_extras'] = self.object.documentos.all()
        
        # 2. Dados Fictícios para o Dashboard (Futuramente você importa dos apps financeiro_fit/agenda_fit)
        # Exemplo: context['total_servicos_ativos'] = ServicoRecorrente.objects.filter(aluno=self.object, ativo=True).count()
        context['qtd_recorrentes'] = 1 
        context['qtd_pacotes_fixos'] = 0
        context['qtd_pacotes_personal'] = 0
        
        return context
    
def upload_documento_extra(request, pk):
    """Recebe o upload do Modal e salva vinculado ao Aluno (pk)"""
    aluno = get_object_or_404(Aluno, pk=pk)
    
    if request.method == 'POST':
        form = DocumentoExtraForm(request.POST, request.FILES)
        if form.is_valid():
            doc = form.save(commit=False)
            doc.aluno = aluno  # Vincula ao aluno da página
            doc.save()
            # Opcional: Mensagem de sucesso
    
    # Volta para a mesma ficha do aluno
    return redirect('aluno_detail', pk=pk)