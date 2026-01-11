from django.db import models
from dateutil.relativedelta import relativedelta  # Precisa instalar: pip install python-dateutil
from core.models import Organizacao
from cadastros_fit.models import Aluno, Profissional, Unidade

# ==============================================================================
# 1. TEMPLATES DE CONTRATO
# ==============================================================================
class TemplateContrato(models.Model):
    organizacao = models.ForeignKey(Organizacao, on_delete=models.CASCADE)
    nome = models.CharField(max_length=100, help_text="Ex: Contrato Padrão 2025")
    
    # Campo que guarda o HTML do contrato com um valor padrão bonitinho
    texto_html = models.TextField(help_text="Use {{aluno.nome}}, {{valor}}, {{data_inicio}} como variáveis.", default="""
        <div style="text-align: center;">
            <img src="https://via.placeholder.com/150" alt="Logo" style="max-height: 100px;">
            <h2>CONTRATO DE PRESTAÇÃO DE SERVIÇOS</h2>
        </div>
        <p>Eu, {{ aluno.nome }}, CPF {{ aluno.cpf }}...</p>
    """)
    
    ativo = models.BooleanField(default=True)

    def __str__(self):
        return self.nome

# ==============================================================================
# 2. PLANOS / PACOTES
# ==============================================================================
class Plano(models.Model):
    # Definição das opções para o dropdown funcionar
    DURACAO_CHOICES = [
        (1, 'Mensal (1 Mês)'),
        (3, 'Trimestral (3 Meses)'),
        (6, 'Semestral (6 Meses)'),
        (12, 'Anual (12 Meses)'),
    ]
    
    # Se precisar de organização por plano, descomente abaixo
    # organizacao = models.ForeignKey(Organizacao, on_delete=models.CASCADE) 
    
    nome = models.CharField(max_length=100, help_text="Ex: Pilates Solo 2x - Trimestral")

    descricao = models.TextField(blank=True, null=True, verbose_name="Descrição do Plano")
    
    valor_mensal = models.DecimalField(max_digits=10, decimal_places=2, help_text="Valor base mensal (sem descontos)")
    
    frequencia_semanal = models.PositiveIntegerField(default=2, help_text="Quantas vezes por semana?")
    
    # Aqui ligamos as opções ao campo
    duracao_meses = models.IntegerField(choices=DURACAO_CHOICES, help_text="1=Mensal, 3=Trimestral, etc")
    
    ativo = models.BooleanField(default=True)
    tipo_servico = models.ForeignKey(
        'cadastros_fit.TipoServico', 
        on_delete=models.SET_NULL, 
        null=True, 
        blank=True,
        verbose_name="Tipo de Serviço"
    )
    @property
    def valor_total_sugerido(self):
        return self.valor_mensal * self.duracao_meses

    def __str__(self):
        return f"{self.nome} ({self.duracao_meses} meses)"

# ==============================================================================
# 3. CONTRATO (A VENDA)
# ==============================================================================
import uuid # <--- Adicione este import no topo do arquivo!

class Contrato(models.Model):
    STATUS_CHOICES = [
        ('PENDENTE', 'Pendente Assinatura'),
        ('ENVIADO_EMAIL', 'Enviado p/ Assinatura (E-mail)'),
        ('ASSINADO_DIGITAL', 'Assinado Digitalmente (Email)'),
        ('ASSINADO_PRESENCIAL', 'Assinado Presencialmente (Tablet)'),
        ('ASSINADO_PAPEL', 'Assinado no Papel'),
        ('CANCELADO', 'Cancelado'),
        ('ENCERRADO', 'Encerrado (Fim do Prazo)'),
    ]

    aluno = models.ForeignKey(Aluno, on_delete=models.CASCADE, related_name='contratos')
    plano = models.ForeignKey(Plano, on_delete=models.PROTECT)
    unidade = models.ForeignKey(Unidade, on_delete=models.PROTECT, verbose_name="Unidade de Atendimento")
    
    # Datas
    data_inicio = models.DateField()
    data_fim = models.DateField(blank=True, null=True)
    dia_vencimento = models.PositiveIntegerField(default=10, help_text="Dia do mês para vencer o boleto/pix")
    
    # Financeiro da Venda
    valor_total = models.DecimalField(max_digits=10, decimal_places=2, default=0.00, help_text="Valor FINAL fechado")
    qtde_parcelas = models.PositiveIntegerField(default=1, help_text="Em quantas vezes será gerado o financeiro?")
    
    # Controle e Assinatura
    status = models.CharField(max_length=30, choices=STATUS_CHOICES, default='PENDENTE')
    template_usado = models.ForeignKey(TemplateContrato, on_delete=models.SET_NULL, null=True, blank=True)
    
    # Campos de Assinatura Digital
    token_assinatura = models.UUIDField(default=uuid.uuid4, editable=False, unique=True, null=False)
# Tirei o unique=True
    assinatura_imagem = models.TextField(blank=True, null=True, help_text="Imagem Base64 da assinatura")
    data_assinatura = models.DateTimeField(blank=True, null=True)
    ip_assinatura = models.GenericIPAddressField(blank=True, null=True)
    
    arquivo_assinado = models.FileField(upload_to='contratos/assinados/', blank=True, null=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    def save(self, *args, **kwargs):
        # 1. Valor padrão se vazio
        if not self.valor_total and self.plano:
            self.valor_total = self.plano.valor_total_sugerido
            
        # 2. Parcelas padrão se vazio
        if not self.qtde_parcelas and self.plano:
            self.qtde_parcelas = self.plano.duracao_meses

        # 3. Calcula Data Fim Automaticamente
        if self.data_inicio and self.plano:
            self.data_fim = self.data_inicio + relativedelta(months=self.plano.duracao_meses)
            
        super().save(*args, **kwargs)

    def __str__(self):
        return f"Contrato {self.id} - {self.aluno.nome}"

# ==============================================================================
# 4. HORÁRIOS FIXOS DA GRADE
# ==============================================================================
class HorarioFixo(models.Model):
    """
    Define a Grade Fixa do aluno.
    Ex: Se o plano é 2x, terá 2 registros aqui vinculados ao contrato.
    """
    DIAS_SEMANA = [
        (0, 'Segunda-feira'),
        (1, 'Terça-feira'),
        (2, 'Quarta-feira'),
        (3, 'Quinta-feira'),
        (4, 'Sexta-feira'),
        (5, 'Sábado'),
        (6, 'Domingo'),
    ]
    
    contrato = models.ForeignKey(Contrato, on_delete=models.CASCADE, related_name='horarios_fixos')
    dia_semana = models.IntegerField(choices=DIAS_SEMANA)
    horario = models.TimeField()
    profissional = models.ForeignKey(Profissional, on_delete=models.SET_NULL, null=True)
    
    def __str__(self):
        return f"{self.get_dia_semana_display()} às {self.horario}"