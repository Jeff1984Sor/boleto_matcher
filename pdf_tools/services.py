import io
import os
import zipfile
import time
import uuid
import json
from pypdf import PdfReader, PdfWriter
import google.generativeai as genai
from django.conf import settings

genai.configure(api_key=settings.GOOGLE_API_KEY)

def extract_text_from_pdf(pdf_path):
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
            if "429" in str(e):
                time.sleep(10)
            else:
                break
    return {"valor": 0.0, "identificador": ""}

def processar_conciliacao_json_stream(lista_caminhos_boletos, caminho_comprovantes, user):
    
    # Helper para enviar JSON no stream
    def send_event(type, data):
        return json.dumps({'type': type, 'data': data}) + "\n"

    yield send_event('log', '🚀 Iniciando IA...')
    
    total_paginas_contadas = 0
    comprovantes_map = []
    
    # A. LER COMPROVANTES
    yield send_event('log', '📂 Lendo Comprovantes...')
    reader_comp = PdfReader(caminho_comprovantes)
    total_paginas_contadas += len(reader_comp.pages)
    
    for i, page in enumerate(reader_comp.pages):
        yield send_event('comp_status', {'index': i, 'msg': 'Analisando...'})
        
        texto_pg = page.extract_text()
        dados = analisar_com_gemini(texto_pg, "comprovante")
        time.sleep(4)
        
        writer_temp = PdfWriter()
        writer_temp.add_page(page)
        pdf_bytes = io.BytesIO()
        writer_temp.write(pdf_bytes)
        pdf_bytes.seek(0)
        
        comprovantes_map.append({'page_obj': pdf_bytes, 'dados': dados, 'usado': False})

    # B. LER BOLETOS
    yield send_event('log', '📂 Processando Boletos...')
    output_zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(output_zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for idx, boleto_path in enumerate(lista_caminhos_boletos):
            nome_arquivo = os.path.basename(boleto_path)
            
            yield send_event('file_start', {'filename': nome_arquivo})
            
            temp_reader = PdfReader(boleto_path)
            total_paginas_contadas += len(temp_reader.pages)
            
            texto_boleto = extract_text_from_pdf(boleto_path)
            dados_boleto = analisar_com_gemini(texto_boleto, "boleto")
            time.sleep(4)

            # Match
            comprovante_match = None
            for comp in comprovantes_map:
                if not comp['usado']:
                    v1 = float(dados_boleto.get('valor') or 0)
                    v2 = float(comp['dados'].get('valor') or 0)
                    if v1 > 0 and abs(v1 - v2) < 0.05:
                        comprovante_match = comp
                        comp['usado'] = True
                        break
            
            match_status = 'success' if comprovante_match else 'warning'
            yield send_event('file_done', {'filename': nome_arquivo, 'status': match_status})

            # Monta PDF
            writer_final = PdfWriter()
            reader_bol = PdfReader(boleto_path)
            for p in reader_bol.pages:
                writer_final.add_page(p)
            
            if comprovante_match:
                reader_match = PdfReader(comprovante_match['page_obj'])
                writer_final.add_page(reader_match.pages[0])
            
            pdf_output = io.BytesIO()
            writer_final.write(pdf_output)
            zip_file.writestr(nome_arquivo, pdf_output.getvalue())

    # C. FINALIZA
    yield send_event('log', '💾 Finalizando...')
    
    pasta_downloads = os.path.join(settings.MEDIA_ROOT, 'downloads')
    os.makedirs(pasta_downloads, exist_ok=True)
    
    nome_zip = f"Resultado_{uuid.uuid4().hex[:8]}.zip"
    caminho_zip_final = os.path.join(pasta_downloads, nome_zip)
    
    with open(caminho_zip_final, "wb") as f:
        f.write(output_zip_buffer.getvalue())
        
    user.paginas_processadas += total_paginas_contadas
    user.save()
    
    url_download = f"{settings.MEDIA_URL}downloads/{nome_zip}"
    
    yield send_event('finish', {'url': url_download, 'total': total_paginas_contadas})