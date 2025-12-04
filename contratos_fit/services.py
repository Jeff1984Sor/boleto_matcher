from datetime import timedelta
from django.utils import timezone
from dateutil.relativedelta import relativedelta
from django.db.models import Sum
import requests
import json
from django.conf import settings

from agenda_fit.models import Aula, Presenca
from financeiro_fit.models import Lancamento, CategoriaFinanceira, ContaBancaria

def processar_novo_contrato(contrato):
    """Gera tudo do zero (usado na venda)"""
    gerar_agenda(contrato)
    gerar_financeiro(contrato)

def regenerar_contrato(contrato):
    """
    Recalcula parcelas e aulas após edição.
    Preserva o passado (pagos/realizados) e recria o futuro.
    """
    print(f"♻️ Regenerando Contrato #{contrato.id}...")
    
    # 1. AGENDA: Remove presenças futuras e recria
    agora = timezone.now()
    Presenca.objects.filter(
        aluno=contrato.aluno,
        aula__data_hora_inicio__gte=agora, # Apenas aulas futuras
        # Se quiser ligar a presença ao contrato específico, precisaria de um campo na Presença. 
        # Assumindo que o aluno só tem 1 contrato ativo por vez:
    ).delete()
    
    # Recria agenda a partir de HOJE (ou da data inicio se for futura)
    data_inicio_recalculo = max(contrato.data_inicio, agora.date())
    gerar_agenda(contrato, data_inicio_forcada=data_inicio_recalculo)


    # 2. FINANCEIRO: Remove pendentes e recalcula saldo
    # Remove parcelas pendentes deste contrato
    Lancamento.objects.filter(contrato=contrato, status='PENDENTE').delete()
    
    # Calcula quanto já foi pago
    total_pago = Lancamento.objects.filter(
        contrato=contrato, 
        status='PAGO'
    ).aggregate(Sum('valor'))['valor__sum'] or 0
    
    parcelas_pagas = Lancamento.objects.filter(contrato=contrato, status='PAGO').count()
    
    # Novo saldo a receber
    saldo_restante = contrato.valor_total - total_pago
    parcelas_restantes = contrato.qtde_parcelas - parcelas_pagas
    
    if saldo_restante > 0 and parcelas_restantes > 0:
        gerar_financeiro(contrato, valor_custom=saldo_restante, qtde_custom=parcelas_restantes, inicio_custom=parcelas_pagas)
    
    print("✅ Contrato regenerado com sucesso!")


# --- FUNÇÕES AUXILIARES ---

def gerar_agenda(contrato, data_inicio_forcada=None):
    horarios = contrato.horarios_fixos.all()
    if not horarios.exists(): return

    data_atual = data_inicio_forcada or contrato.data_inicio
    data_fim = contrato.data_fim
    
    # Evita loop infinito se datas estiverem erradas
    if data_atual > data_fim: return

    while data_atual <= data_fim:
        dia_semana_atual = data_atual.weekday()
        
        for h in horarios:
            if h.dia_semana == dia_semana_atual:
                inicio = timezone.make_aware(timezone.datetime.combine(data_atual, h.horario))
                fim = inicio + timedelta(hours=1)
                
                aula, _ = Aula.objects.get_or_create(
                    data_hora_inicio=inicio,
                    profissional=h.profissional,
                    defaults={
                         # Atenção se removeu campo organizacao do plano
                        'unidade': contrato.unidade,
                        'data_hora_fim': fim,
                        'status': 'AGENDADA'
                    }
                )
                
                if aula.presencas.count() < aula.capacidade_maxima:
                    if not aula.presencas.filter(aluno=contrato.aluno).exists():
                        Presenca.objects.create(aula=aula, aluno=contrato.aluno)
        
        data_atual += timedelta(days=1)

def gerar_financeiro(contrato, valor_custom=None, qtde_custom=None, inicio_custom=0):
    valor_total = valor_custom if valor_custom is not None else contrato.valor_total
    qtde = qtde_custom if qtde_custom is not None else contrato.qtde_parcelas
    
    if valor_total <= 0 or qtde <= 0: return

    valor_parcela = valor_total / qtde
    
    # Busca Categoria/Conta (ajuste conforme seu banco)
    # Como removemos organizacao do Plano, pegamos do Contrato.aluno.organizacao ou do request (mas aqui é service)
    # Assumindo que o contrato tem unidade e unidade tem acesso ou o schema já define.
    # No multi-tenant, Category.objects.first() pega a categoria do schema atual.
    
    categoria = CategoriaFinanceira.objects.filter(tipo='RECEITA').first()
    conta = ContaBancaria.objects.first()
    
    if not categoria or not conta: return # Evita crash

    for i in range(qtde):
        numero_parcela = inicio_custom + i + 1
        
        # Data Vencimento
        data_venc = contrato.data_inicio + relativedelta(months=(numero_parcela - 1))
        try:
            data_venc = data_venc.replace(day=contrato.dia_vencimento)
        except ValueError:
            data_venc = data_venc + relativedelta(day=31)
            
        # Não gera boleto no passado se já venceu há muito tempo? (Opcional)
        
        Lancamento.objects.create(
            descricao=f"Mensalidade {numero_parcela}/{contrato.qtde_parcelas} - {contrato.plano.nome}",
            aluno=contrato.aluno,
            contrato=contrato,
            categoria=categoria,
            conta=conta,
            valor=valor_parcela,
            data_vencimento=data_venc,
            status='PENDENTE'
        )

def enviar_contrato_n8n(contrato):
    """
    Envia os dados do contrato para o N8N processar a assinatura digital.
    """
    if not contrato.aluno.email:
        print("⚠️ Aluno sem e-mail. Pulei o envio para N8N.")
        return False

    webhook_url = "https://seu-n8n.com/webhook/assinatura-contrato" # <--- COLOCAR SUA URL DO N8N
    
    payload = {
        "contrato_id": contrato.id,
        "aluno_nome": contrato.aluno.nome,
        "aluno_email": contrato.aluno.email,
        "aluno_cpf": contrato.aluno.cpf,
        "plano": contrato.plano.nome,
        "valor": str(contrato.valor_total),
        "data_inicio": str(contrato.data_inicio),
        # Link para o N8N chamar de volta quando assinar
        "callback_url": f"https://studio.mayacorp.com.br/api/contratos/{contrato.id}/assinado/" 
    }

    try:
        requests.post(webhook_url, json=payload, timeout=5)
        contrato.status = 'ENVIADO_EMAIL'
        contrato.save()
        return True
    except Exception as e:
        print(f"❌ Erro ao chamar N8N: {e}")
        return False