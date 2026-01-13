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
from django.db.models import Count, Q
from django.db.models.functions import TruncMonth
import calendar
from django.db.models.functions import ExtractMonth
from django.views.generic import TemplateView

# Imports Locais
from cadastros_fit.models import Aluno
from .models import Aula, Presenca, ConfiguracaoIntegracao
from .forms import IntegracaoForm

from django.views.generic import TemplateView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.db.models import Count
from django.db.models.functions import ExtractMonth
from django.utils import timezone
import calendar
from .models import Aula, Presenca
from django.views.decorators.csrf import csrf_exempt
from django.db.models import Count, Q, Sum
from django.utils import timezone
from datetime import datetime
from agenda_fit.models import Aula, Presenca
from cadastros_fit.models import Aluno
# Se você tiver o serviço TotalPass, mantenha. Se não, comente para evitar erro.
# from .services_totalpass import TotalPassService

# ==============================================================================
# 1. AGENDA SEMANAL (CALENDÁRIO GERAL)
# ==============================================================================
API_KEY_N8N = "segredo_mayacorp_n8n_123" 

@login_required
def calendario_semanal(request):
    # =========================================================
    # 1. DATA BASE
    # =========================================================
    data_get = request.GET.get('data')
    if data_get:
        data_base = timezone.datetime.strptime(data_get, '%Y-%m-%d').date()
    else:
        data_base = timezone.now().date()

    inicio_semana = data_base - timedelta(days=data_base.weekday())
    fim_semana = inicio_semana + timedelta(days=6)

    # =========================================================
    # 2. PROFISSIONAL (ID DO USER)
    # =========================================================
    prof_raw = request.GET.get('prof_id')

    if prof_raw and prof_raw not in ('all', 'None', '') and prof_raw.isdigit():
        prof_id = int(prof_raw)
    else:
        prof_id = 'all'

    # =========================================================
    # 3. PRESENÇAS BASE
    # =========================================================
    presencas = Presenca.objects.filter(
        aula__data_hora_inicio__date__gte=inicio_semana,
        aula__data_hora_inicio__date__lte=fim_semana
    ).select_related(
        'aula',
        'aluno',
        'aula__profissional',
        'aula__profissional__user',  # ✅ CERTO
    )

    # =========================================================
    # 4. FILTRO POR PROFISSIONAL (USER)
    # =========================================================
    if prof_id != 'all':
        presencas = presencas.filter(
            aula__profissional__user_id=prof_id
        )

    # =========================================================
    # 5. LISTA DE PROFISSIONAIS (SEM usuario)
    # =========================================================
    lista_profissionais = (
        Profissional.objects
        .filter(ativo=True)
        .select_related('user')          # ✅ CERTO
        .order_by('user__first_name')    # ✅ CERTO
    )

    # =========================================================
    # 6. GRADE SEMANAL
    # =========================================================
    dias_da_semana = []
    grade_semanal = {i: [] for i in range(7)}
    hoje = timezone.now().date()

    for i in range(7):
        dia = inicio_semana + timedelta(days=i)
        dias_da_semana.append({
            'data': dia,
            'hoje': dia == hoje
        })

    for p in presencas:
        grade_semanal[p.aula.data_hora_inicio.weekday()].append(p)

    # =========================================================
    # 7. CONTEXTO FINAL
    # =========================================================
    context = {
        'dias_da_semana': dias_da_semana,
        'grade_semanal': grade_semanal,
        'inicio_semana': inicio_semana,
        'fim_semana': fim_semana,
        'prox_semana': (inicio_semana + timedelta(days=7)).strftime('%Y-%m-%d'),
        'ant_semana': (inicio_semana - timedelta(days=7)).strftime('%Y-%m-%d'),
        'lista_profissionais': lista_profissionais,
        'prof_selecionado': prof_id,
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

class DashboardAulasView(LoginRequiredMixin, TemplateView):
    template_name = 'agenda_fit/dashboard_aulas.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        hoje = timezone.now()
        try:
            ano = int(self.request.GET.get('ano', hoje.year))
            mes = int(self.request.GET.get('mes', hoje.month))
        except ValueError:
            ano = hoje.year
            mes = hoje.month
        
        _, last_day = calendar.monthrange(ano, mes)
        inicio_mes = timezone.datetime(ano, mes, 1).date()
        fim_mes = timezone.datetime(ano, mes, last_day).date()
        
        context['ano_atual'] = ano
        context['mes_atual'] = mes
        context['anos_select'] = range(hoje.year - 2, hoje.year + 3)

        # --- GRÁFICO 1: AULAS POR PROFISSIONAL (ANUAL) ---
        aulas_ano = Aula.objects.filter(
            data_hora_inicio__year=ano,
            status='REALIZADA'
        ).annotate(mes=ExtractMonth('data_hora_inicio')) \
         .values('mes', 'profissional__nome') \
         .annotate(total=Count('id'))
        
        dados_profs = {}
        for item in aulas_ano:
            nome = item['profissional__nome'] or "Sem Prof."
            mes_idx = item['mes'] - 1
            if nome not in dados_profs:
                dados_profs[nome] = [0] * 12
            dados_profs[nome][mes_idx] = item['total']
            
        datasets_prof = []
        cores = ['#0d6efd', '#198754', '#dc3545', '#ffc107', '#0dcaf0', '#6610f2', '#fd7e14', '#20c997']
        i = 0
        for nome, dados in dados_profs.items():
            cor = cores[i % len(cores)]
            datasets_prof.append({
                'label': nome,
                'data': dados,
                'borderColor': cor,
                'backgroundColor': cor,
                'tension': 0.4,
                'fill': False
            })
            i += 1
        context['chart_prof_datasets'] = datasets_prof

        # --- GRÁFICO 2: FREQUÊNCIA (PRESENÇAS vs FALTAS - ANUAL) ---
        frequencia_ano = Presenca.objects.filter(
            aula__data_hora_inicio__year=ano
        ).annotate(mes=ExtractMonth('aula__data_hora_inicio')) \
         .values('mes', 'status') \
         .annotate(total=Count('id'))
         
        data_presente = [0] * 12
        data_falta = [0] * 12
        
        for item in frequencia_ano:
            idx = item['mes'] - 1
            if item['status'] == 'PRESENTE':
                data_presente[idx] = item['total']
            elif item['status'] == 'FALTA':
                data_falta[idx] = item['total']
        
        context['chart_presente'] = data_presente
        context['chart_falta'] = data_falta

        # --- INDICADORES MENSAIS ---
        context['aulas_restantes'] = Aula.objects.filter(
            data_hora_inicio__date__range=[timezone.now().date(), fim_mes],
            status='AGENDADA'
        ).count()

        context['top_assiduos'] = Presenca.objects.filter(
            aula__data_hora_inicio__date__range=[inicio_mes, fim_mes],
            status='PRESENTE'
        ).values('aluno__nome').annotate(total=Count('id')).order_by('-total')[:5]

        context['top_faltosos'] = Presenca.objects.filter(
            aula__data_hora_inicio__date__range=[inicio_mes, fim_mes],
            status='FALTA'
        ).values('aluno__nome').annotate(total=Count('id')).order_by('-total')[:5]

        return context
    
@csrf_exempt
def api_agenda_amanha(request):
    token = request.headers.get('X-API-KEY')
    if token != API_KEY_N8N:
        return JsonResponse({'erro': 'Acesso negado'}, status=403)

    amanha = timezone.now().date() + timedelta(days=1)
    
    aulas = Aula.objects.filter(
        data_hora_inicio__date=amanha
    ).exclude(status='CANCELADA').select_related('profissional').prefetch_related('presencas__aluno')

    dados_envio = {}

    for aula in aulas:
        prof = aula.profissional
        if not prof.email: continue
            
        if prof.id not in dados_envio:
            dados_envio[prof.id] = {
                "profissional": prof.nome,
                "email": prof.email,
                "data": amanha.strftime('%d/%m/%Y'),
                "aulas": []
            }
        
        alunos_lista = [p.aluno.nome for p in aula.presencas.all()]
        if not alunos_lista: alunos_lista = ["Vaga livre"]

        dados_envio[prof.id]["aulas"].append({
            "horario": aula.data_hora_inicio.strftime('%H:%M'),
            "alunos": ", ".join(alunos_lista)
        })

    return JsonResponse(list(dados_envio.values()), safe=False)

@login_required
def performance_aulas(request):
    hoje = timezone.now()
    
    # 1. Capturar Filtros
    mes_sel = int(request.GET.get('mes', hoje.month))
    ano_sel = int(request.GET.get('ano', hoje.year))
    prof_id = request.GET.get('prof_id')

    # 2. Base de Filtro para Aulas e Presenças
    filtros_aula = Q(data_hora_inicio__month=mes_sel, data_hora_inicio__year=ano_sel)
    filtros_presenca = Q(aula__data_hora_inicio__month=mes_sel, aula__data_hora_inicio__year=ano_sel)

    if prof_id and prof_id != 'all':
        filtros_aula &= Q(profissional_id=prof_id)
        filtros_presenca &= Q(aula__profissional_id=prof_id)

    # 3. Cálculo de KPIs
    aulas_realizadas = Aula.objects.filter(filtros_aula, status='REALIZADA').count()

    # Ocupação: Presenças vs Vagas
    dados_ocupacao = Aula.objects.filter(filtros_aula, status='REALIZADA').aggregate(
        total_vagas=Sum('capacidade_maxima'),
        total_presencas=Count('presencas', filter=Q(presencas__status='PRESENTE'))
    )
    vagas = dados_ocupacao['total_vagas'] or 1
    presencas = dados_ocupacao['total_presencas'] or 0
    taxa_ocupacao = round((presencas / vagas) * 100)

    # Faltas
    faltas_mes = Presenca.objects.filter(filtros_presenca, status='FALTA').count()

    # Novos Alunos (Geral do Studio no período)
    novos_alunos = Aluno.objects.filter(criado_em__month=mes_sel, criado_em__year=ano_sel).count()

    # 4. Top Alunos (Ranking Real)
    top_alunos = Presenca.objects.filter(filtros_presenca, status='PRESENTE').values(
        'aluno__nome'
    ).annotate(
        total_aulas=Count('id')
    ).order_by('-total_aulas')[:5]

    # 5. Lista de Profissionais para o Filtro
    profissionais = Profissional.objects.filter(ativo=True)

    context = {
        'aulas_realizadas': aulas_realizadas,
        'taxa_ocupacao': taxa_ocupacao,
        'faltas_mes': faltas_mes,
        'novos_alunos': novos_alunos,
        'top_alunos': top_alunos,
        'profissionais': profissionais,
        'mes_sel': mes_sel,
        'ano_sel': ano_sel,
        'prof_sel': int(prof_id) if prof_id and prof_id != 'all' else 'all',
    }
    
    return render(request, 'agenda_fit/performance.html', context)