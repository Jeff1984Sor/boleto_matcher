import io
import zipfile
import time  # <--- Importante para o Delay
from pypdf import PdfReader, PdfWriter
import google.generativeai as genai
import json
from django.conf import settings

# Configura a chave aqui ou garante que está no settings
genai.configure(api_key=settings.GOOGLE_API_KEY)

def extract_text_from_pdf(pdf_file):
    reader = PdfReader(pdf_file)
    text = ""
    for page in reader.pages:
        text += page.extract_text() + "\n"
    return text

def analisar_com_gemini(texto, tipo_doc):
    """
    Usa o modelo FLASH (mais rápido e com limite maior) e tem Retry automático.
    """
    # Usa o modelo 2.0 Flash que sua conta tem acesso
    model = genai.GenerativeModel('gemini-2.0-flash')
    
    prompt = f"""
    Analise o texto abaixo extraído de um {tipo_doc}.
    Retorne APENAS um objeto JSON (sem markdown) com os campos:
    - "valor": (float, use ponto para decimais, ex: 150.50)
    - "identificador": (string, codigo de barras ou nome. Algo único).
    
    Texto:
    {texto[:4000]}
    """
    
    # SISTEMA DE TENTATIVAS (RETRY)
    # Se der erro 429, ele espera e tenta de novo até 3 vezes
    for tentativa in range(1, 4):
        try:
            response = model.generate_content(prompt)
            clean_text = response.text.replace('```json', '').replace('```', '').strip()
            return json.loads(clean_text)
        except Exception as e:
            print(f"Tentativa {tentativa} falhou: {e}")
            if "429" in str(e):
                print("Limite atingido. Esperando 10 segundos...")
                time.sleep(10) # Espera 10s antes de tentar de novo
            else:
                break # Se for outro erro, para

    # Se falhar tudo
    return {"valor": 0.0, "identificador": ""}

def processar_conciliacao(lista_boletos, arquivo_comprovantes):
    # A. Ler Comprovantes
    comprovantes_map = []
    reader_comp = PdfReader(arquivo_comprovantes)
    
    print(f"Processando {len(reader_comp.pages)} comprovantes...")
    
    for i, page in enumerate(reader_comp.pages):
        texto_pg = page.extract_text()
        
        # Chama a IA
        dados = analisar_com_gemini(texto_pg, "comprovante")
        print(f"Comprovante {i+1}: {dados}")
        
        # DELAY PREVENTIVO
        # Espera 4 segundos entre cada página para não irritar o Google
        time.sleep(4) 
        
        writer_temp = PdfWriter()
        writer_temp.add_page(page)
        pdf_bytes = io.BytesIO()
        writer_temp.write(pdf_bytes)
        pdf_bytes.seek(0)
        
        comprovantes_map.append({
            'page_obj': pdf_bytes,
            'dados': dados,
            'usado': False
        })

    # B. Ler Boletos
    output_zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(output_zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        
        for boleto_file in lista_boletos:
            texto_boleto = extract_text_from_pdf(boleto_file)
            dados_boleto = analisar_com_gemini(texto_boleto, "boleto")
            
            print(f"Boleto processado: {dados_boleto}")
            time.sleep(4) # Delay também nos boletos
            
            # Lógica de Matching
            comprovante_match = None
            for comp in comprovantes_map:
                if not comp['usado']:
                    val_bol = float(dados_boleto.get('valor') or 0)
                    val_comp = float(comp['dados'].get('valor') or 0)
                    
                    # Match por valor (margem de 5 centavos)
                    if val_bol > 0 and abs(val_bol - val_comp) < 0.05:
                        comprovante_match = comp
                        comp['usado'] = True
                        break
            
            # Cria o PDF fusionado
            writer_final = PdfWriter()
            
            # Pág 1: Boleto
            boleto_file.seek(0)
            reader_bol = PdfReader(boleto_file)
            for p in reader_bol.pages:
                writer_final.add_page(p)
            
            # Pág 2: Comprovante (se achou)
            if comprovante_match:
                reader_match = PdfReader(comprovante_match['page_obj'])
                writer_final.add_page(reader_match.pages[0])
            
            # --- NOME DO ARQUIVO ORIGINAL ---
            nome_arquivo = boleto_file.name
            
            # Salva no ZIP
            pdf_output = io.BytesIO()
            writer_final.write(pdf_output)
            zip_file.writestr(nome_arquivo, pdf_output.getvalue())

    output_zip_buffer.seek(0)
    return output_zip_buffer