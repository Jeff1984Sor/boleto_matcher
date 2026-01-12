import requests
import json

def enviar_mensagem_evolution(organizacao, telefone, mensagem):
    from .models import ConexaoWhatsapp
    config = ConexaoWhatsapp.objects.filter(organizacao=organizacao, ativo=True).first()
    
    if not config:
        return False, "Configuração de API não encontrada ou inativa."

    # Limpa o telefone
    telefone_limpo = "".join(filter(str.isdigit, telefone))
    if not telefone_limpo.startswith("55"):
        telefone_limpo = "55" + telefone_limpo
    
    # Garante que a URL não termine com / para não duplicar na concatenação
    url_base = config.url_base.rstrip('/')
    url = f"{url_base}/message/sendText/{config.instancia}"
    
    # Payload ajustado para maior compatibilidade com Evolution API
    payload = {
        "number": telefone_limpo,
        "options": {
            "delay": 1200, 
            "presence": "composing", 
            "linkPreview": False
        },
        "text": mensagem  # Em algumas versões da Evolution é 'text', em outras 'textMessage'
    }
    
    headers = {
        "Content-Type": "application/json",
        "apikey": config.apikey
    }

    try:
        # Usar json=payload o requests já trata o Content-Type e serialização
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        
        # Log de debug se precisar: print(response.text)
        return response.status_code in [200, 201], response.text
    except Exception as e:
        return False, str(e)