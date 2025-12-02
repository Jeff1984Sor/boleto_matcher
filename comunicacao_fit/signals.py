from django.db.models.signals import post_save
from django.dispatch import receiver
from cadastros_fit.models import Aluno
import requests
import threading
import json

# IMPORTANTE: Coloque aqui a URL do Webhook do seu n8n
# Como você está usando o tunnel, pegue a URL que começa com https://....hooks.n8n.cloud
N8N_WEBHOOK_URL = "https://dry-octopus-5.hooks.n8n.cloud/webhook-test/comunicacao"

def enviar_n8n_background(payload):
    """Envia os dados para o n8n em segundo plano para não travar o cadastro"""
    try:
        requests.post(N8N_WEBHOOK_URL, json=payload, timeout=10)
    except Exception as e:
        print(f"[ERRO N8N] Falha ao enviar: {e}")

@receiver(post_save, sender=Aluno)
def gatilho_boas_vindas(sender, instance, created, **kwargs):
    """
    Sempre que um Aluno é criado (created=True), dispara mensagem.
    """
    if created and instance.telefone:
        payload = {
            "tipo": "boas_vindas",
            "nome": instance.nome,
            "telefone": instance.telefone,
            # Pode mandar mais dados se quiser
        }
        # Dispara thread para não deixar o usuário esperando a resposta da API
        threading.Thread(target=enviar_n8n_background, args=(payload,)).start()