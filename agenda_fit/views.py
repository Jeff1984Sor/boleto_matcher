from django.shortcuts import render

# Create your views here.
from datetime import timedelta
from django.shortcuts import render
from django.utils import timezone
from cadastros_fit.models import Aluno
from django.contrib.auth.decorators import login_required
from .models import Aula, Presenca
from django.shortcuts import get_object_or_404, redirect
from django.contrib import messages
from .models import ConfiguracaoIntegracao
from .forms import IntegracaoForm
from django.views.generic import UpdateView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.urls import reverse_lazy
from .models import ConfiguracaoIntegracao
from .forms import IntegracaoForm
from django.http import JsonResponse
from .services_totalpass import TotalPassService
from cadastros_fit.models import Aluno
from .models import Aula, Presenca

@login_required
def calendario_semanal(request):
    # 1. Determina a data base (hoje ou passada via GET)
    data_get = request.GET.get('data')
    if data_get:
        data_base = timezone.datetime.strptime(data_get, '%Y-%m-%d').date()
    else:
        data_base = timezone.now().date()

    # 2. Calcula o início (Segunda) e fim (Domingo) da semana
    inicio_semana = data_base - timedelta(days=data_base.weekday())
    fim_semana = inicio_semana + timedelta(days=6)

    # 3. Busca as aulas do período
    aulas = Aula.objects.filter(
        data_hora_inicio__date__gte=inicio_semana,
        data_hora_inicio__date__lte=fim_semana
    ).select_related('profissional', 'unidade').order_by('data_hora_inicio')

    # 4. Organiza os dados para o Template (Dicionário: {0: [aulas_seg], 1: [aulas_ter]...})
    # Criamos também uma lista de cabeçalhos com as datas dos dias
    dias_da_semana = []
    grade_semanal = {i: [] for i in range(7)} # 0 a 6
    
    for i in range(7):
        dia_atual = inicio_semana + timedelta(days=i)
        dias_da_semana.append({
            'data': dia_atual,
            'nome': dia_atual.strftime('%A'), # Nome do dia (ex: Monday) - vamos traduzir no template
            'hoje': dia_atual == timezone.now().date()
        })

    for aula in aulas:
        dia_index = aula.data_hora_inicio.weekday()
        grade_semanal[dia_index].append(aula)

    # Navegação (Semana anterior e próxima)
    prox_semana = (inicio_semana + timedelta(days=7)).strftime('%Y-%m-%d')
    ant_semana = (inicio_semana - timedelta(days=7)).strftime('%Y-%m-%d')

    context = {
        'dias_da_semana': dias_da_semana, # Cabeçalhos
        'grade_semanal': grade_semanal,   # Conteúdo
        'inicio_semana': inicio_semana,
        'fim_semana': fim_semana,
        'prox_semana': prox_semana,
        'ant_semana': ant_semana,
    }

    return render(request, 'agenda_fit/calendario_semanal.html', context)

@login_required
def acao_marcar_presenca(request, presenca_id):
    p = get_object_or_404(Presenca, id=presenca_id)
    p.status = 'PRESENTE'
    p.save()
    messages.success(request, "Presença confirmada!")
    return redirect('lista_aulas_aluno', aluno_id=p.aluno.id)

@login_required
def acao_marcar_realizada(request, presenca_id):
    p = get_object_or_404(Presenca, id=presenca_id)
    # Marca a AULA inteira como realizada
    aula = p.aula
    aula.status = 'REALIZADA'
    aula.save()
    messages.success(request, "Aula marcada como Realizada!")
    return redirect('lista_aulas_aluno', aluno_id=p.aluno.id)

@login_required
def acao_deletar_agendamento(request, presenca_id):
    p = get_object_or_404(Presenca, id=presenca_id)
    aluno_id = p.aluno.id
    p.delete()
    messages.warning(request, "Agendamento removido.")
    return redirect('lista_aulas_aluno', aluno_id=aluno_id)

@login_required
def acao_remarcar_aula(request, presenca_id):
    # Lógica simples: Remove a atual e manda para o calendário para escolher outra
    p = get_object_or_404(Presenca, id=presenca_id)
    aluno_id = p.aluno.id
    p.delete() # Remove a atual
    messages.info(request, "Agendamento anterior removido. Escolha o novo horário no calendário.")
    return redirect('calendario_semanal')

@login_required
def lista_aulas_aluno(request, aluno_id):
    aluno = get_object_or_404(Aluno, pk=aluno_id)
    presencas = Presenca.objects.filter(aluno=aluno).select_related('aula', 'aula__profissional').order_by('-aula__data_hora_inicio')
    
    return render(request, 'agenda_fit/aluno_aulas_list.html', {
        'aluno': aluno,
        'presencas': presencas,
        'agora': timezone.now() # <--- ADICIONE ISSO
    })

@login_required
def gerenciar_aula(request, aula_id):
    aula = get_object_or_404(Aula, id=aula_id)
    
    if request.method == 'POST':
        # 1. Salva a Evolução (Texto do que foi feito na aula)
        evolucao = request.POST.get('evolucao_texto')
        aula.evolucao_texto = evolucao
        
        # Se todos foram atendidos, marca aula como REALIZADA
        # (Você pode refinar essa lógica depois)
        aula.status = 'REALIZADA'
        aula.save()

        # 2. Salva a Chamada (Presença de cada aluno)
        # O form vai mandar dados como: presenca_10 = 'PRESENTE', presenca_12 = 'FALTA'
        for presenca in aula.presencas.all():
            key = f"status_{presenca.id}" # Nome do input no HTML
            novo_status = request.POST.get(key)
            
            if novo_status:
                presenca.status = novo_status
                presenca.save()
        
        messages.success(request, "Chamada e evolução salvas com sucesso!")
        return redirect('calendario_semanal') # Ou volta para a mesma aula

    return render(request, 'agenda_fit/gerenciar_aula.html', {
        'aula': aula
    })

class ConfiguracaoIntegracaoView(LoginRequiredMixin, UpdateView):
    model = ConfiguracaoIntegracao
    form_class = IntegracaoForm
    template_name = 'agenda_fit/config_integracao.html'
    success_url = reverse_lazy('home') # Ou volta pra mesma tela

    def get_object(self, queryset=None):
        # Pega a primeira configuração ou cria se não existir
        obj, created = ConfiguracaoIntegracao.objects.get_or_create(pk=1)
        return obj
    
@login_required
def checkin_totalpass(request):
    if request.method == "POST":
        aluno_id = request.POST.get('aluno_id')
        token = request.POST.get('token_totalpass')
        aula_id = request.POST.get('aula_id') # Opcional: Se já quiser vincular na aula

        aluno = get_object_or_404(Aluno, pk=aluno_id)

        # 1. Chama a API da TotalPass
        resultado = TotalPassService.validar_token(token, aluno.cpf)

        if resultado['sucesso']:
            # 2. Se a TotalPass aprovou, marca presença no seu sistema
            if aula_id:
                aula = get_object_or_404(Aula, pk=aula_id)
                Presenca.objects.create(
                    aula=aula, 
                    aluno=aluno, 
                    status='PRESENTE'
                )
                # Opcional: Salvar no Log Financeiro como "A Receber TotalPass"
            
            return JsonResponse({'status': 'ok', 'msg': resultado['mensagem']})
        else:
            return JsonResponse({'status': 'error', 'msg': resultado['mensagem']}, status=400)

    return JsonResponse({'status': 'error', 'msg': 'Método inválido'}, status=405)

def lista_aulas_aluno(request, aluno_id):
    # Simples redirect para o calendário por enquanto, ou cria uma lista
    return redirect('calendario_semanal')