from django.db import models
from core.models import Organizacao
from cadastros_fit.models import Aluno
from contratos_fit.models import Contrato

class CategoriaFinanceira(models.Model):
    TIPO_CHOICES = [('RECEITA', 'Receita'), ('DESPESA', 'Despesa')]
    
    nome = models.CharField(max_length=100, help_text="Ex: Mensalidades, Aluguel, Luz")
    tipo = models.CharField(max_length=10, choices=TIPO_CHOICES)
    
    def __str__(self):
        return f"{self.nome} ({self.get_tipo_display()})"

class ContaBancaria(models.Model):
    nome = models.CharField(max_length=100, help_text="Ex: Cofre, Nubank, Itaú")
    saldo_atual = models.DecimalField(max_digits=12, decimal_places=2, default=0.00)
    
    def __str__(self):
        return f"{self.nome} - R$ {self.saldo_atual}"

class Lancamento(models.Model):
    STATUS_CHOICES = [('PENDENTE', 'Pendente'), ('PAGO', 'Pago'), ('CANCELADO', 'Cancelado')]
    FORMA_PGTO = [('PIX', 'Pix'), ('DINHEIRO', 'Dinheiro'), ('CARTAO', 'Cartão'), ('BOLETO', 'Boleto')]

    descricao = models.CharField(max_length=200)
    
    # Vínculos (Opcionais, pois pode ser uma despesa de luz sem aluno)
    aluno = models.ForeignKey(Aluno, on_delete=models.SET_NULL, null=True, blank=True)
    contrato = models.ForeignKey(Contrato, on_delete=models.SET_NULL, null=True, blank=True)
    categoria = models.ForeignKey(CategoriaFinanceira, on_delete=models.PROTECT)
    conta = models.ForeignKey(ContaBancaria, on_delete=models.PROTECT, verbose_name="Conta/Caixa")
    
    valor = models.DecimalField(max_digits=10, decimal_places=2)
    data_vencimento = models.DateField()
    data_pagamento = models.DateField(null=True, blank=True)
    forma_pagamento = models.CharField(max_length=20, choices=FORMA_PGTO, blank=True, null=True)
    
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDENTE')
    
    criado_em = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.descricao} - R$ {self.valor} ({self.status})"