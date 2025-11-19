import io
import os # <--- Adicionado
import zipfile
import time
from pypdf import PdfReader, PdfWriter
import google.generativeai as genai
import json
from django.conf import settings

genai.configure(api_key=settings.GOOGLE_API_KEY)

def extract_text_from_pdf(pdf_path):
    # Agora abrimos o arquivo do disco
    reader = PdfReader(pdf_path)
    text = ""
    for page in reader.pages:
        text += page.extract_text() + "\n"
    return text

def analisar_com_gemini(texto, tipo_doc):
    model = genai.GenerativeModel('gemini-2.0-flash')
    
    prompt = f"""
    Analise o texto abaixo extraído de um {tipo_doc}.
    Retorne APENAS um objeto JSON (sem markdown) com os campos:
    - "valor": (float, use ponto para decimais, ex: 150.50)
    - "identificador": (string, codigo de barras ou nome. Algo único).
    
    Texto:
    {texto[:4000]}
    """
    
    for tentativa in range(1, 4):
        try:
            response = model.generate_content(prompt)
            clean_text = response.text.replace('```json', '').replace('```', '').strip()
            return json.loads(clean_text)
        except Exception as e:
            print(f"Tentativa {tentativa} falhou: {e}")
            if "429" in str(e):
                time.sleep(10)
            else:
                break
    return {"valor": 0.0, "identificador": ""}

def processar_conciliacao(lista_caminhos_boletos, caminho_comprovantes):
    total_paginas_contadas = 0

    # A. Ler Comprovantes (Lendo do disco)
    comprovantes_map = []
    reader_comp = PdfReader(caminho_comprovantes)
    
    total_paginas_contadas += len(reader_comp.pages)
    
    for i, page in enumerate(reader_comp.pages):
        texto_pg = page.extract_text()
        dados = analisar_com_gemini(texto_pg, "comprovante")
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
        
        for boleto_path in lista_caminhos_boletos:
            # Ler do disco
            temp_reader = PdfReader(boleto_path)
            total_paginas_contadas += len(temp_reader.pages)
            
            texto_boleto = extract_text_from_pdf(boleto_path)
            dados_boleto = analisar_com_gemini(texto_boleto, "boleto")
            time.sleep(4)
            
            # Matching
            comprovante_match = None
            for comp in comprovantes_map:
                if not comp['usado']:
                    val_bol = float(dados_boleto.get('valor') or 0)
                    val_comp = float(comp['dados'].get('valor') or 0)
                    
                    if val_bol > 0 and abs(val_bol - val_comp) < 0.05:
                        comprovante_match = comp
                        comp['usado'] = True
                        break
            
            # Cria PDF Final
            writer_final = PdfWriter()
            
            reader_bol = PdfReader(boleto_path)
            for p in reader_bol.pages:
                writer_final.add_page(p)
            
            if comprovante_match:
                reader_match = PdfReader(comprovante_match['page_obj'])
                writer_final.add_page(reader_match.pages[0])
            
            # PEGA O NOME DO ARQUIVO A PARTIR DO CAMINHO
            nome_arquivo = os.path.basename(boleto_path)
            
            pdf_output = io.BytesIO()
            writer_final.write(pdf_output)
            zip_file.writestr(nome_arquivo, pdf_output.getvalue())

    output_zip_buffer.seek(0)
    return output_zip_buffer, total_paginas_contadas