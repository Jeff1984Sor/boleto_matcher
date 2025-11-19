from django.contrib.auth.models import AbstractUser
from django.db import models

# 1. Criar a tabela de Produtos
class Produto(models.Model):
    nome = models.CharField(max_length=100)
    slug = models.SlugField(unique=True, help_text="Identificador único no código (ex: gerador-pdf)")
    descricao = models.TextField(blank=True)

    def __str__(self):
        return self.nome

# 2. Atualizar o Usuário
class CustomUser(AbstractUser):
    telefone = models.CharField(max_length=15, blank=True, null=True, verbose_name="Telefone/WhatsApp")
    cpf = models.CharField(max_length=14, blank=True, null=True, verbose_name="CPF")
    nome_empresa = models.CharField(max_length=100, blank=True, null=True, verbose_name="Nome da Empresa")
    
    # Mantemos isso para controle geral
    is_assinante = models.BooleanField(default=False, verbose_name="É Assinante?")
    
    # NOVO: Lista de produtos que esse usuário comprou
    produtos = models.ManyToManyField(Produto, blank=True, verbose_name="Produtos Contratados")

    paginas_processadas = models.PositiveIntegerField(default=0, verbose_name="Páginas Analisadas")

    def __str__(self):
        return self.username
    
class HistoricoConsumo(models.Model):
    usuario = models.ForeignKey(CustomUser, on_delete=models.CASCADE, related_name='historico')
    data_fechamento = models.DateField(auto_now_add=True, verbose_name="Data do Fechamento")
    paginas_no_ciclo = models.PositiveIntegerField(verbose_name="Páginas Usadas")

    def __str__(self):
        return f"{self.usuario} - {self.paginas_no_ciclo} pgs em {self.data_fechamento.strftime('%m/%Y')}"
    
class BannerHome(models.Model):
    titulo = models.CharField(max_length=200)
    subtitulo = models.CharField(max_length=300, blank=True)
    imagem = models.ImageField(upload_to='banners/', blank=True, null=True, help_text="Tamanho ideal: 1200x400px")
    link_botao = models.CharField(max_length=200, blank=True, help_text="Ex: /tools/pdf/ ou https://google.com")
    texto_botao = models.CharField(max_length=50, default="Saiba Mais")
    ativo = models.BooleanField(default=True)
    ordem = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ['ordem']

    def __str__(self):
        return self.titulo
    