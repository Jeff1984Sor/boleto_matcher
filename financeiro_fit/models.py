from django.db import models
import uuid
from cadastros_fit.models import Aluno, Profissional
from contratos_fit.models import Contrato

# ==============================================================================
# 1. CADASTROS BÁSICOS
# ==============================================================================

class CategoriaFinanceira(models.Model):
    TIPO_CHOICES = [('RECEITA', 'Receita'), ('DESPESA', 'Despesa')]
    
    nome = models.CharField(max_length=100)
    tipo = models.CharField(max_length=10, choices=TIPO_CHOICES)
    
    # Subcategoria (Ex: Despesa > Fixa > Aluguel)
    categoria_pai = models.ForeignKey('self', on_delete=models.CASCADE, null=True, blank=True, related_name='subcategorias', verbose_name="Categoria Pai")
    
    def __str__(self):
        if self.categoria_pai:
            return f"{self.categoria_pai.nome} > {self.nome}"
        return f"{self.nome} ({self.get_tipo_display()})"
    
    class Meta:
        verbose_name = "Categoria Financeira"
        verbose_name_plural = "Categorias Financeiras"
        ordering = ['tipo', 'categoria_pai__nome', 'nome']

class ContaBancaria(models.Model):
    nome = models.CharField(max_length=100, help_text="Ex: Cofre, Nubank, Itaú")
    saldo_atual = models.DecimalField(max_digits=12, decimal_places=2, default=0.00)
    
    def __str__(self):
        return f"{self.nome} - R$ {self.saldo_atual}"

class Fornecedor(models.Model):
    nome = models.CharField("Razão Social / Nome", max_length=200)
    nome_fantasia = models.CharField(max_length=200, blank=True, null=True)
    cnpj_cpf = models.CharField("CNPJ/CPF", max_length=20, blank=True, null=True)
    telefone = models.CharField(max_length=20, blank=True, null=True)
    email = models.EmailField(blank=True, null=True)
    
    # Dados Bancários do Fornecedor (Opcional, para fazer PIX pra ele)
    chave_pix = models.CharField(max_length=100, blank=True, null=True)
    
    ativo = models.BooleanField(default=True)

    def __str__(self):
        return self.nome_fantasia or self.nome

# ==============================================================================
# 2. LANÇAMENTOS (CONTAS A PAGAR E RECEBER)
# ==============================================================================

class Lancamento(models.Model):
    STATUS_CHOICES = [('PENDENTE', 'Pendente'), ('PAGO', 'Pago'), ('CANCELADO', 'Cancelado')]
    FORMA_PGTO = [
        ('PIX', 'Pix'), 
        ('DINHEIRO', 'Dinheiro'), 
        ('CARTAO_CREDITO', 'Cartão de Crédito'), 
        ('CARTAO_DEBITO', 'Cartão de Débito'), 
        ('BOLETO', 'Boleto'),
        ('TRANSFERENCIA', 'Transferência')
    ]

    # Identificação
    descricao = models.CharField("Descrição", max_length=200)
    
    # Recorrência (Para saber se essa conta faz parte de um carnê/parcelamento)
    grupo_serie = models.UUIDField(null=True, blank=True, help_text="ID que agrupa parcelas recorrentes")
    parcela_atual = models.PositiveIntegerField(default=1, help_text="1/12")
    total_parcelas = models.PositiveIntegerField(default=1, help_text="12")

    # Vínculos (Quem paga ou quem recebe)
    aluno = models.ForeignKey(Aluno, on_delete=models.SET_NULL, null=True, blank=True, help_text="Se for Mensalidade")
    fornecedor = models.ForeignKey(Fornecedor, on_delete=models.SET_NULL, null=True, blank=True, help_text="Se for Despesa (Luz, Água)")
    profissional = models.ForeignKey(Profissional, on_delete=models.SET_NULL, null=True, blank=True, help_text="Se for Pagamento de Salário")
    contrato = models.ForeignKey(Contrato, on_delete=models.SET_NULL, null=True, blank=True)
    
    # Classificação
    categoria = models.ForeignKey(CategoriaFinanceira, on_delete=models.PROTECT)
    conta = models.ForeignKey(ContaBancaria, on_delete=models.PROTECT, verbose_name="Conta/Caixa de Saída/Entrada")
    
    # Valores e Datas
    valor = models.DecimalField(max_digits=10, decimal_places=2)
    data_vencimento = models.DateField()
    data_pagamento = models.DateField(null=True, blank=True)
    forma_pagamento = models.CharField(max_length=20, choices=FORMA_PGTO, blank=True, null=True)
    
    # Arquivos e Comprovantes
    arquivo_boleto = models.FileField(upload_to='financeiro/boletos/', blank=True, null=True, verbose_name="Boleto (PDF)")
    arquivo_comprovante = models.FileField(upload_to='financeiro/comprovantes/', blank=True, null=True, verbose_name="Comprovante de Pgto")
    
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDENTE')
    criado_em = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        tipo = "Receita" if self.categoria.tipo == 'RECEITA' else "Despesa"
        return f"[{tipo}] {self.descricao} - R$ {self.valor}"