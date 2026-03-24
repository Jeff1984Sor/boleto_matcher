from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login, get_user_model
from django.contrib.auth import login
from .forms import CustomUserCreationForm
from .models import BannerHome
from django.contrib.auth.decorators import login_required
from .decorators import possui_produto
from .models import CustomUser
from .forms import UsuarioSistemaForm
from django.contrib import messages
from django.contrib.auth import authenticate, login
from django.http import HttpResponse
from django.db import connection
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.utils import timezone
from django.views.generic import ListView, CreateView, UpdateView
from django.contrib.auth.models import User
from django.urls import reverse_lazy
from django.contrib.auth.mixins import LoginRequiredMixin


# Essa função agora manda o HTML completo (com menu)
def home(request):
    hoje = timezone.now().date()
    
    # Pega os banners (da sua segunda função antiga)
    banners = BannerHome.objects.filter(ativo=True)
    
    # Prepara o contexto com os dados do dashboard (da sua primeira função antiga)
    context = {
        'total_alunos': 0,
        'aulas_hoje': 0,
        'receber_hoje': 0,
        'banners': banners,
    }
    return render(request, 'home.html', context)

def cadastro(request):
    if request.method == 'POST':
        form = CustomUserCreationForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            return redirect('home')
    else:
        form = CustomUserCreationForm()
    
    return render(request, 'registration/cadastro.html', {'form': form})

@login_required
@possui_produto('gestao-pilates')
def lista_usuarios(request):
    # Só mostra usuários da MESMA organização
    usuarios = CustomUser.objects.filter(organizacao=request.user.organizacao)
    return render(request, 'core/lista_usuarios.html', {'usuarios': usuarios})

@login_required
@possui_produto('gestao-pilates')
def novo_usuario_sistema(request):
    if request.method == 'POST':
        form = UsuarioSistemaForm(request.POST)
        if form.is_valid():
            user = form.save(commit=False)
            # Vincula o novo usuário à organização do chefe
            user.organizacao = request.user.organizacao
            user.save()
            messages.success(request, "Usuário criado com sucesso!")
            return redirect('lista_usuarios')
    else:
        form = UsuarioSistemaForm()
    
    return render(request, 'core/form_usuario.html', {'form': form})

def debug_auth(request):
    u_txt = 'suporte'
    p_txt = '123'

    User = get_user_model()
    html = "<h2>Diagnostico</h2>"
    try:
        user_db = User.objects.get(username=u_txt)
        html += f"<p style='color:blue'>OK 1. Usuario encontrado (ID: {user_db.id}).</p>"

        if user_db.check_password(p_txt):
             html += "<p style='color:blue'>OK 2. Senha bate.</p>"
        else:
             html += "<p style='color:red'>ERRO 2. Senha errada.</p>"

        user_auth = authenticate(request, username=u_txt, password=p_txt)
        if user_auth:
            login(request, user_auth)
            html += "<h1 style='color:green'>LOGIN SUCESSO!</h1> <a href='/admin/'>ENTRAR</a>"
        else:
            html += "<h1 style='color:orange'>WARN Authenticate falhou</h1>"

    except User.DoesNotExist:
        html += "<p style='color:red'>ERRO Usuario nao existe.</p>"

    return HttpResponse(html)

from django.shortcuts import render

def performance_aulas(request):
    """Página de performance de aulas - em desenvolvimento"""
    context = {
        'title': 'Performance de Aulas - Studio',
        # adicione seus dados aqui depois
    }
    return render(request, 'core/performance_aulas.html', context)

class UsuarioListView(LoginRequiredMixin, ListView):
    model = User
    template_name = 'core/lista_usuarios.html'
    context_object_name = 'usuarios' # Nome que você vai usar no {% for u in usuarios %}

class UsuarioCreateView(LoginRequiredMixin, CreateView):
    model = User
    fields = ['username', 'email', 'first_name', 'last_name', 'is_staff', 'is_active']
    template_name = 'core/form_usuario.html'
    success_url = reverse_lazy('core:lista_usuarios')

class UsuarioUpdateView(LoginRequiredMixin, UpdateView):
    model = User
    fields = ['username', 'email', 'first_name', 'last_name', 'is_staff', 'is_active']
    template_name = 'core/form_usuario.html'
    success_url = reverse_lazy('core:lista_usuarios')

def dashboard_view(request):
    # Aqui você pode buscar dados para o dashboard depois
    context = {
        'total_alunos': 0, # Exemplo: Aluno.objects.count()
        'proximas_aulas': [], # Exemplo: Aula.objects.filter(...)
    }
    return render(request, 'core/dashboard.html', context)
