from django.db import models

# ==============================================================================
# 1. ESTRUTURA BÁSICA
# ==============================================================================

class Unidade(models.Model):
    # REMOVIDO: organizacao = ForeignKey... (O schema já define a organização)
    nome = models.CharField(max_length=100)
    endereco = models.CharField(max_length=255, blank=True, null=True)
    telefone = models.CharField(max_length=20, blank=True)
    
    def __str__(self):
        return self.nome

class Profissional(models.Model):
    # REMOVIDO: organizacao = ForeignKey...
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
def formatar_nome(nome):
    excecoes = ['da', 'de', 'do', 'das', 'dos', 'e']
    palavras = nome.lower().split()
    nome_formatado = [p if p in excecoes else p.capitalize() for p in palavras]
    return " ".join(nome_formatado)

class Aluno(models.Model):
    # --- DADOS PESSOAIS ---
    nome = models.CharField(max_length=100)
    cpf = models.CharField(max_length=14, unique=True, null=True, blank=True) # CPF deve ser único se existir
    data_nascimento = models.DateField(blank=True, null=True)
    telefone = models.CharField(max_length=20, blank=True)
    email = models.EmailField(blank=True)
    
    # Documento de Identificação (Digitalizado)
    doc_identidade_foto = models.ImageField(upload_to='alunos/docs_pessoais/', blank=True, null=True, verbose_name="Foto RG/CNH")

    # --- ENDEREÇO (Quebrado para facilitar) ---
    cep = models.CharField(max_length=9, blank=True)
    logradouro = models.CharField(max_length=150, blank=True, verbose_name="Rua/Av")
    numero = models.CharField(max_length=20, blank=True, verbose_name="Número")
    complemento = models.CharField(max_length=100, blank=True)
    bairro = models.CharField(max_length=100, blank=True)
    cidade = models.CharField(max_length=100, blank=True)
    estado = models.CharField(max_length=2, blank=True) # UF (SP, RJ...)
    
    # Comprovante de Endereço (Digitalizado)
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
        if self.nome:
            self.nome = formatar_nome(self.nome)
        if self.logradouro:
            self.logradouro = formatar_nome(self.logradouro)
        if self.bairro:
            self.bairro = formatar_nome(self.bairro)
        if self.cidade:
            self.cidade = formatar_nome(self.cidade)
        if self.estado:
            self.estado = self.estado.upper() # Estado sempre MAIÚSCULO (SP, RJ)
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