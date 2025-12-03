from datetime import timedelta
from django.shortcuts import render, get_object_or_404, redirect
from django.utils import timezone
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic import ListView, UpdateView
from django.urls import reverse_lazy
from django.http import JsonResponse
from django.contrib import messages
from django.utils.dateparse import parse_datetime
from cadastros_fit.models import Profissional

# Imports Locais
from cadastros_fit.models import Aluno
from .models import Aula, Presenca, ConfiguracaoIntegracao
from .forms import IntegracaoForm
# Se você tiver o serviço TotalPass, mantenha. Se não, comente para evitar erro.
# from .services_totalpass import TotalPassService

# ==============================================================================
# 1. AGENDA SEMANAL (CALENDÁRIO GERAL)
# ==============================================================================

@login_required
def calendario_semanal(request):
    # 1. Data Base
    data_get = request.GET.get('data')
    if data_get:
        data_base = timezone.datetime.strptime(data_get, '%Y-%m-%d').date()
    else:
        data_base = timezone.now().date()

    inicio_semana = data_base - timedelta(days=data_base.weekday())
    fim_semana = inicio_semana + timedelta(days=6)

    # 2. Busca Aulas (Base)
    aulas = Aula.objects.filter(
        data_hora_inicio__date__gte=inicio_semana,
        data_hora_inicio__date__lte=fim_semana
    ).select_related('profissional', 'unidade').order_by('data_hora_inicio')

    # --- NOVO: FILTRO POR PROFISSIONAL ---
    prof_id = request.GET.get('prof_id')
    if prof_id:
        aulas = aulas.filter(profissional_id=prof_id)
    
    # Pega lista para o dropdown
    lista_profissionais = Profissional.objects.filter(ativo=True)
    # -------------------------------------

    # 3. Organiza Grade
    dias_da_semana = []
    grade_semanal = {i: [] for i in range(7)}
    
    for i in range(7):
        dia_atual = inicio_semana + timedelta(days=i)
        dias_da_semana.append({
            'data': dia_atual,
            'nome': dia_atual.strftime('%A'),
            'hoje': dia_atual == timezone.now().date()
        })

    for aula in aulas:
        dia_index = aula.data_hora_inicio.weekday()
        grade_semanal[dia_index].append(aula)

    prox_semana = (inicio_semana + timedelta(days=7)).strftime('%Y-%m-%d')
    ant_semana = (inicio_semana - timedelta(days=7)).strftime('%Y-%m-%d')

    context = {
        'dias_da_semana': dias_da_semana,
        'grade_semanal': grade_semanal,
        'inicio_semana': inicio_semana,
        'fim_semana': fim_semana,
        'prox_semana': prox_semana,
        'ant_semana': ant_semana,
        'lista_profissionais': lista_profissionais, # Envia para o template
        'prof_selecionado': int(prof_id) if prof_id else None
    }

    return render(request, 'agenda_fit/calendario_semanal.html', context)

# ==============================================================================
# 2. AÇÕES DE AULA (BOTÕES)
# ==============================================================================

@login_required
def confirmar_presenca(request, pk):
    p = get_object_or_404(Presenca, pk=pk)
    p.status = 'PRESENTE'
    p.save()
    messages.success(request, "Presença confirmada!")
    # Redireciona de volta para onde veio (Aluno ou Calendário)
    return redirect(request.META.get('HTTP_REFERER', 'calendario_semanal'))

@login_required
def cancelar_presenca(request, pk):
    p = get_object_or_404(Presenca, pk=pk)
    # Remove a presença (libera vaga)
    p.delete()
    messages.warning(request, "Agendamento cancelado.")
    return redirect(request.META.get('HTTP_REFERER', 'calendario_semanal'))

@login_required
def remarcar_aula(request, pk):
    presenca = get_object_or_404(Presenca, pk=pk)
    
    if request.method == 'POST':
        nova_data_str = request.POST.get('nova_data')
        if nova_data_str:
            nova_data = parse_datetime(nova_data_str)
            
            # Cria nova aula ou usa existente
            nova_aula, created = Aula.objects.get_or_create(
                data_hora_inicio=nova_data,
                # Assume 1h
                data_hora_fim=nova_data + timedelta(hours=1),
                profissional=presenca.aula.profissional,
                defaults={
                    'unidade': presenca.aula.unidade, # Ajuste se precisar de tenant
                    'status': 'AGENDADA'
                }
            )
            
            # Move o aluno
            presenca.aula = nova_aula
            presenca.status = 'AGENDADA' # Reseta status se estava com falta
            presenca.save()
            
            messages.success(request, f"Remarcado para {nova_data.strftime('%d/%m %H:%M')}")
        else:
            messages.error(request, "Data inválida")
            
    return redirect(request.META.get('HTTP_REFERER', 'calendario_semanal'))

@login_required
def gerenciar_aula(request, aula_id):
    """Tela para o professor fazer a chamada e evolução"""
    aula = get_object_or_404(Aula, id=aula_id)
    
    if request.method == 'POST':
        # 1. Processa as Presenças PRIMEIRO para saber se alguém veio
        teve_presenca = False
        
        for presenca in aula.presencas.all():
            key = f"status_{presenca.id}"
            novo_status = request.POST.get(key)
            
            if novo_status:
                presenca.status = novo_status
                presenca.save()
                
                # Verifica se pelo menos um aluno está presente
                if novo_status == 'PRESENTE':
                    teve_presenca = True

        # 2. Atualiza o Status da Aula
        # Só marca como REALIZADA se houve presença confirmada
        if teve_presenca:
            aula.status = 'REALIZADA'
        else:
            # Se todos faltaram, a aula não foi "Realizada" tecnicamente
            # Mantemos como estava (AGENDADA) ou mudamos para CANCELADA se preferir
            # Por enquanto, vou manter como estava para não sumir da tela
            pass

        # 3. Salva a Evolução
        aula.evolucao_texto = request.POST.get('evolucao_texto')
        aula.save()
        
        messages.success(request, "Chamada salva com sucesso!")
        return redirect('calendario_semanal')

    # GET: Se for abrir numa pagina separada (backup do modal)
    return render(request, 'agenda_fit/gerenciar_aula.html', {'aula': aula})

# ==============================================================================
# 3. AGENDA ESPECÍFICA DO ALUNO
# ==============================================================================

@login_required
def lista_aulas_aluno(request, aluno_id):
    aluno = get_object_or_404(Aluno, pk=aluno_id)
    # Traz histórico completo
    presencas = Presenca.objects.filter(aluno=aluno).select_related('aula', 'aula__profissional').order_by('-aula__data_hora_inicio')
    
    return render(request, 'agenda_fit/lista_aulas_aluno.html', {
        'aluno': aluno, 
        'presencas': presencas
    })

# ==============================================================================
# 4. RELATÓRIOS
# ==============================================================================

class RelatorioFrequenciaView(LoginRequiredMixin, ListView):
    model = Presenca
    template_name = 'agenda_fit/relatorio_frequencia.html'
    context_object_name = 'presencas'
    ordering = ['-aula__data_hora_inicio']

    def get_queryset(self):
        queryset = super().get_queryset()
        
        aluno_id = self.request.GET.get('aluno')
        data_inicio = self.request.GET.get('data_inicio')
        data_fim = self.request.GET.get('data_fim')
        status = self.request.GET.get('status')

        if aluno_id:
            queryset = queryset.filter(aluno_id=aluno_id)
        if data_inicio:
            queryset = queryset.filter(aula__data_hora_inicio__date__gte=data_inicio)
        if data_fim:
            queryset = queryset.filter(aula__data_hora_inicio__date__lte=data_fim)
        if status:
            queryset = queryset.filter(status=status)

        return queryset.select_related('aluno', 'aula', 'aula__profissional')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['alunos_list'] = Aluno.objects.filter(ativo=True).order_by('nome')
        return context

# ==============================================================================
# 5. CONFIGURAÇÕES & INTEGRAÇÕES
# ==============================================================================

class ConfiguracaoIntegracaoView(LoginRequiredMixin, UpdateView):
    model = ConfiguracaoIntegracao
    form_class = IntegracaoForm
    template_name = 'agenda_fit/config_integracao.html'
    success_url = reverse_lazy('home')

    def get_object(self, queryset=None):
        obj, created = ConfiguracaoIntegracao.objects.get_or_create(pk=1)
        return obj
    
@login_required
def checkin_totalpass(request):
    if request.method == "POST":
        return JsonResponse({'status': 'ok', 'msg': 'Simulação OK'})
        # Implementar lógica real quando tiver as credenciais
    return JsonResponse({'status': 'error', 'msg': 'Método inválido'}, status=405)