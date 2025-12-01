from django.db import models

# Create your models here.
from django.db import models
from core.models import Organizacao
from cadastros_fit.models import Aluno, Profissional, Unidade

class Aula(models.Model):
    STATUS_CHOICES = [
        ('AGENDADA', 'Agendada'),
        ('CONFIRMADA', 'Confirmada (Whats)'),
        ('REALIZADA', 'Realizada'),
        ('CANCELADA', 'Cancelada'),
    ]
    
    unidade = models.ForeignKey(Unidade, on_delete=models.CASCADE)
    profissional = models.ForeignKey(Profissional, on_delete=models.SET_NULL, null=True)
    
    data_hora_inicio = models.DateTimeField()
    data_hora_fim = models.DateTimeField()
    
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='AGENDADA')
    capacidade_maxima = models.PositiveIntegerField(default=3)
    
    # Evolução (O que foi feito na aula)
    evolucao_texto = models.TextField(blank=True, verbose_name="Evolução / Prontuário")
    
    criado_em = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Aula {self.data_hora_inicio.strftime('%d/%m %H:%M')} - {self.profissional}"

class Presenca(models.Model):
    STATUS_PRESENCA = [
        ('PRESENTE', 'Presente'),
        ('FALTA', 'Falta'),
        ('FALTA_JUSTIFICADA', 'Falta Justificada (Repõe)'),
    ]
    
    aula = models.ForeignKey(Aula, on_delete=models.CASCADE, related_name='presencas')
    aluno = models.ForeignKey(Aluno, on_delete=models.CASCADE)
    status = models.CharField(max_length=20, choices=STATUS_PRESENCA, default='PRESENTE')
    
    def __str__(self):
        return f"{self.aluno} em {self.aula}"

class MacroEvolucao(models.Model):
    organizacao = models.ForeignKey(Organizacao, on_delete=models.CASCADE)
    titulo = models.CharField(max_length=100)
    texto_padrao = models.TextField()
    
    def __str__(self):
        return self.titulo