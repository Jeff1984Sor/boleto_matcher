import os
import django

# --- Configuração para acessar o settings.py do Django ---
# Ajuste 'seu_projeto.settings' para o nome real da pasta do seu projeto.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mayacorp.settings')
django.setup()
# ---------------------------------------------------------

import google.generativeai as genai
from django.conf import settings

print("🔑 Verificando modelos disponíveis para sua API Key...")

try:
    # Configura a API Key a partir do seu arquivo de configurações do Django
    genai.configure(api_key=settings.GOOGLE_API_KEY)

    print("\n--- Modelos Disponíveis ---")
    
    # Itera sobre todos os modelos e encontra aqueles que suportam 'generateContent'
    for model in genai.list_models():
        if 'generateContent' in model.supported_generation_methods:
            print(f"✅ {model.name}")

    print("\n---------------------------\n")
    print("💡 Você pode usar qualquer um dos modelos listados acima no seu código.")
    print("   - Para sua tarefa de ler imagens, os recomendados são 'gemini-1.5-pro-latest' e 'gemini-1.5-flash-latest'.")

except Exception as e:
    print(f"\n❌ Ocorreu um erro ao tentar listar os modelos: {e}")
    print("   Por favor, verifique se a sua GOOGLE_API_KEY no arquivo settings.py está correta e se o faturamento está ativo.")