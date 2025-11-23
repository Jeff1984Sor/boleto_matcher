import google.generativeai as genai
from django.conf import settings
import json
from PIL import Image

# Configura a API Key (Garante que está no settings)
genai.configure(api_key=settings.GOOGLE_API_KEY)

class OCRService:
    
    @staticmethod
    def extrair_dados_identidade(imagem_path_ou_file):
        """
        Lê CNH ou RG e retorna JSON com: nome, cpf, data_nascimento.
        """
        model = genai.GenerativeModel('gemini-2.0-flash')
        
        # Prepara a imagem para o Gemini
        img = Image.open(imagem_path_ou_file)

        prompt = """
        Analise esta imagem de um documento de identidade (CNH ou RG brasileiro).
        Extraia os seguintes dados e retorne APENAS um JSON (sem markdown, sem ```json):
        
        {
            "nome": "Nome Completo",
            "cpf": "000.000.000-00",
            "data_nascimento": "AAAA-MM-DD"
        }
        
        Se não encontrar algum dado, deixe null.
        """
        
        try:
            response = model.generate_content([prompt, img])
            texto_limpo = response.text.replace('```json', '').replace('```', '').strip()
            return json.loads(texto_limpo)
        except Exception as e:
            print(f"Erro OCR Identidade: {e}")
            return {"erro": "Falha ao ler documento"}

    @staticmethod
    def extrair_dados_endereco(imagem_path_ou_file):
        """
        Lê conta de luz/água/net e retorna JSON com endereço completo.
        """
        model = genai.GenerativeModel('gemini-2.0-flash')
        
        img = Image.open(imagem_path_ou_file)

        prompt = """
        Analise esta imagem de um comprovante de residência.
        Extraia o endereço e retorne APENAS um JSON:
        
        {
            "cep": "00000-000",
            "logradouro": "Nome da Rua",
            "numero": "123",
            "bairro": "Bairro",
            "cidade": "Cidade",
            "estado": "UF"
        }
        
        Priorize o endereço de instalação ou do cliente.
        """
        
        try:
            response = model.generate_content([prompt, img])
            texto_limpo = response.text.replace('```json', '').replace('```', '').strip()
            return json.loads(texto_limpo)
        except Exception as e:
            print(f"Erro OCR Endereço: {e}")
            return {"erro": "Falha ao ler comprovante"}