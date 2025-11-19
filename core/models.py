from django.contrib.auth.models import AbstractUser
from django.db import models

class CustomUser(AbstractUser):
    # Dados Pessoais
    telefone = models.CharField(max_length=15, blank=True, null=True, verbose_name="Telefone/WhatsApp")
    cpf = models.CharField(max_length=14, blank=True, null=True, verbose_name="CPF")
    
    # Dados da Empresa (opcional)
    nome_empresa = models.CharField(max_length=100, blank=True, null=True, verbose_name="Nome da Empresa")
    
    # Controle de Acesso (SaaS)
    is_assinante = models.BooleanField(default=False, verbose_name="É Assinante?")
    data_expiracao = models.DateField(null=True, blank=True, verbose_name="Assinatura válida até")

    def __str__(self):
        return self.email if self.email else self.username
    pass