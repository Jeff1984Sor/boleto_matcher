from django.db import models
from core.models import Organizacao

# ==============================================================================
# 1. ESTRUTURA BÁSICA (Unidade vem primeiro para evitar erro de dependência)
# ==============================================================================

class Unidade(models.Model):
    organizacao = models.ForeignKey(Organizacao, on_delete=models.CASCADE, related_name='unidades')
    nome = models.CharField(max_length=100)
    endereco = models.CharField(max_length=255, blank=True)
    telefone = models.CharField(max_length=20, blank=True)
    
    def __str__(self):
        return f"{self.nome} - {self.organizacao.nome}"

class Profissional(models.Model):
    organizacao = models.ForeignKey(Organizacao, on_delete=models.CASCADE)
    nome = models.CharField(max_length=100)
    cpf = models.CharField(max_length=14, unique=True)
    crefito = models.CharField(max_length=20, blank=True, verbose_name="Registro Profissional")
    cor_agenda = models.CharField(max_length=7, default="#007bff", help_text="Cor Hexadecimal para a agenda")
    valor_hora_aula = models.DecimalField(max_digits=10, decimal_places=2, default=0.00, verbose_name="Valor por Aula Dada")
    ativo = models.BooleanField(default=True)

    def __str__(self):
        return self.nome

# ==============================================================================
# 2. ALUNOS
# ==============================================================================

class Aluno(models.Model):
    organizacao = models.ForeignKey(Organizacao, on_delete=models.CASCADE)
    nome = models.CharField(max_length=100)
    cpf = models.CharField(max_length=14, blank=True, null=True)
    data_nascimento = models.DateField(blank=True, null=True)
    telefone = models.CharField(max_length=20, blank=True)
    email = models.EmailField(blank=True)
    endereco = models.TextField(blank=True)
    
    # Saúde
    anamnese = models.JSONField(default=dict, blank=True, verbose_name="Ficha de Saúde")
    
    # Segurança / Catraca
    foto_rosto = models.ImageField(upload_to='alunos/fotos/', blank=True, null=True)
    biometria_template = models.TextField(blank=True, null=True, help_text="Hash da digital ou face para catraca")
    bloqueado_catraca = models.BooleanField(default=False, help_text="Bloqueio manual ou financeiro")
    
    criado_em = models.DateTimeField(auto_now_add=True)
    ativo = models.BooleanField(default=True)

    def __str__(self):
        return self.nome

class DocumentoAluno(models.Model):
    aluno = models.ForeignKey(Aluno, on_delete=models.CASCADE, related_name='documentos')
    titulo = models.CharField(max_length=100)
    arquivo = models.FileField(upload_to='alunos/docs/')
    data_upload = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.titulo

# ==============================================================================
# 3. IOT / CATRACA (Depende de Unidade e Aluno)
# ==============================================================================

class DispositivoAcesso(models.Model):
    # Aqui garantimos que Unidade já foi lida pelo Python
    unidade = models.ForeignKey(Unidade, on_delete=models.CASCADE)
    nome = models.CharField(max_length=50, help_text="Ex: Catraca Entrada")
    ip_address = models.GenericIPAddressField(blank=True, null=True)
    token_api = models.CharField(max_length=100, unique=True, help_text="Token para a catraca se autenticar")
    
    def __str__(self):
        return self.nome

class LogAcesso(models.Model):
    aluno = models.ForeignKey(Aluno, on_delete=models.CASCADE)
    dispositivo = models.ForeignKey(DispositivoAcesso, on_delete=models.SET_NULL, null=True)
    data_hora = models.DateTimeField(auto_now_add=True)
    direcao = models.CharField(max_length=10, choices=[('ENTRADA', 'Entrada'), ('SAIDA', 'Saída')])
    status = models.CharField(max_length=20, choices=[('LIBERADO', 'Liberado'), ('BLOQUEADO', 'Bloqueado')])
    motivo_bloqueio = models.CharField(max_length=100, blank=True)

    def __str__(self):
        return f"{self.aluno} - {self.status} em {self.data_hora}"