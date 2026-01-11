from django.db import models
from django.conf import settings

# Função utilitária para deixar nomes bonitos (Ex: "joão da silva" -> "João da Silva")
def formatar_nome(nome):
    if not nome: return ""
    excecoes = ['da', 'de', 'do', 'das', 'dos', 'e']
    palavras = nome.lower().split()
    nome_formatado = [p if p in excecoes else p.capitalize() for p in palavras]
    return " ".join(nome_formatado)

# ==============================================================================
# 1. ESTRUTURA BÁSICA
# ==============================================================================

class Unidade(models.Model):
    # OBS: Removemos 'organizacao' pois o Schema já isola os dados
    nome = models.CharField(max_length=100)
    endereco = models.CharField(max_length=255, blank=True, null=True)
    telefone = models.CharField(max_length=20, blank=True)
    capacidade_padrao = models.PositiveIntegerField(default=3, verbose_name="Capacidade Padrão por Aula")
    
    def __str__(self):
        return self.nome



class Profissional(models.Model):
    # --- VÍNCULO COM O USUÁRIO DO SISTEMA (CORE) ---
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='perfil_profissional',
        verbose_name="Usuário de Acesso"
    )

    # --- DADOS PESSOAIS ---
    nome = models.CharField("Nome Completo", max_length=100)
    cpf = models.CharField("CPF", max_length=14, unique=True)
    data_nascimento = models.DateField("Data de Nascimento", blank=True, null=True)
    email = models.EmailField("E-mail", max_length=255, blank=True, null=True)
    telefone = models.CharField("Celular/WhatsApp", max_length=20, blank=True, null=True)
    
    PERFIL_CHOICES = [('ADMIN', 'Administrador'), ('INSTRUTOR', 'Instrutor / Prof.')]
    perfil = models.CharField(max_length=20, choices=PERFIL_CHOICES, default='INSTRUTOR')
    
    # --- DADOS PROFISSIONAIS ---
    crefito = models.CharField(max_length=20, blank=True, verbose_name="Registro Profissional")
    cor_agenda = models.CharField(max_length=7, default="#007bff", help_text="Cor Hexadecimal para a agenda")
    valor_hora_aula = models.DecimalField(max_digits=10, decimal_places=2, default=0.00, verbose_name="Valor por Aula Dada")
    
    # --- ENDEREÇO ---
    cep = models.CharField("CEP", max_length=9, blank=True, null=True)
    logradouro = models.CharField("Endereço", max_length=150, blank=True, null=True)
    numero = models.CharField("Número", max_length=20, blank=True, null=True)
    bairro = models.CharField("Bairro", max_length=100, blank=True, null=True)
    cidade = models.CharField("Cidade", max_length=100, blank=True, null=True)
    estado = models.CharField("UF", max_length=2, blank=True, null=True)

    ativo = models.BooleanField(default=True)

    def save(self, *args, **kwargs):
        # Formata para MAIÚSCULO diretamente aqui
        if self.nome: self.nome = self.nome.upper()
        if self.logradouro: self.logradouro = self.logradouro.upper()
        if self.bairro: self.bairro = self.bairro.upper()
        if self.cidade: self.cidade = self.cidade.upper()
        if self.estado: self.estado = self.estado.upper() if self.estado else None
        super().save(*args, **kwargs)

    def __str__(self):
        return self.nome
# ==============================================================================
# 2. ALUNOS
# ==============================================================================

class Aluno(models.Model):
    # --- DADOS PESSOAIS ---
    nome = models.CharField(max_length=100)
    cpf = models.CharField(max_length=14, unique=True, null=True, blank=True) 
    data_nascimento = models.DateField(blank=True, null=True)
    telefone = models.CharField(max_length=20, blank=True)
    email = models.EmailField(blank=True)
    
    # Documento de Identificação
    doc_identidade_foto = models.ImageField(upload_to='alunos/docs_pessoais/', blank=True, null=True, verbose_name="Foto RG/CNH")

    # --- ENDEREÇO ---
    cep = models.CharField(max_length=9, blank=True)
    logradouro = models.CharField(max_length=150, blank=True, verbose_name="Rua/Av")
    numero = models.CharField(max_length=20, blank=True, verbose_name="Número")
    complemento = models.CharField(max_length=100, blank=True)
    bairro = models.CharField(max_length=100, blank=True)
    cidade = models.CharField(max_length=100, blank=True)
    estado = models.CharField(max_length=2, blank=True) # UF (SP, RJ...)
    
    # Comprovante de Residência
    comprovante_residencia_foto = models.ImageField(upload_to='alunos/docs_residencia/', blank=True, null=True)
    
    # --- SAÚDE ---
    anamnese = models.JSONField(default=dict, blank=True, verbose_name="Ficha de Saúde")
    
    # --- SEGURANÇA / CATRACA ---
    foto_rosto = models.ImageField(upload_to='alunos/fotos/', blank=True, null=True)
    biometria_template = models.TextField(blank=True, null=True, help_text="Hash da digital ou face")
    bloqueado_catraca = models.BooleanField(default=False, help_text="Bloqueio financeiro/manual")
    
    criado_em = models.DateTimeField(auto_now_add=True)
    ativo = models.BooleanField(default=True)

    def save(self, *args, **kwargs):
        # Formatação automática antes de salvar
        if self.nome:
            self.nome = formatar_nome(self.nome)
        if self.logradouro:
            self.logradouro = formatar_nome(self.logradouro)
        if self.bairro:
            self.bairro = formatar_nome(self.bairro)
        if self.cidade:
            self.cidade = formatar_nome(self.cidade)
        if self.estado:
            self.estado = self.estado.upper() 
        super().save(*args, **kwargs)

    def __str__(self):
        return self.nome
    
    @property
    def endereco_completo(self):
        return f"{self.logradouro}, {self.numero} - {self.bairro}, {self.cidade}/{self.estado}"

class DocumentoAluno(models.Model):
    aluno = models.ForeignKey(Aluno, on_delete=models.CASCADE, related_name='documentos')
    titulo = models.CharField(max_length=100)
    arquivo = models.FileField(upload_to='alunos/docs/')
    data_upload = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.titulo

# ==============================================================================
# 3. IOT / CATRACA
# ==============================================================================

class DispositivoAcesso(models.Model):
    unidade = models.ForeignKey(Unidade, on_delete=models.CASCADE)
    nome = models.CharField(max_length=50, help_text="Ex: Catraca Entrada")
    ip_address = models.GenericIPAddressField(blank=True, null=True)
    token_api = models.CharField(max_length=100, unique=True, help_text="Token de autenticação")
    
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
        return f"{self.aluno} - {self.status}"
    

class TipoServico(models.Model):
    nome = models.CharField(max_length=100)
    cor = models.CharField(max_length=7, default='#802422') # Nosso Marsala
    ativo = models.BooleanField(default=True)

    def __str__(self):
        return self.nome

    class Meta:
        verbose_name = "Tipo de Serviço"
        verbose_name_plural = "Tipos de Serviço"