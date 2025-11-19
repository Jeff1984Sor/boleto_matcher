import io
import os
import zipfile
import time
import uuid
from pypdf import PdfReader, PdfWriter
import google.generativeai as genai
import json
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

# --- MUDANÇA AQUI: Generator (yield) ---
def processar_conciliacao_stream(lista_caminhos_boletos, caminho_comprovantes, user):
    yield f'<div style="font-family: monospace; background: #000; color: #0f0; padding: 20px;">'
    yield f'<p>🚀 Iniciando o motor de Inteligência Artificial...</p>'
    
    total_paginas_contadas = 0
    comprovantes_map = []
    
    # A. LER COMPROVANTES
    yield f'<p>📂 Lendo arquivo de comprovantes...</p>'
    reader_comp = PdfReader(caminho_comprovantes)
    total_paginas_contadas += len(reader_comp.pages)
    
    for i, page in enumerate(reader_comp.pages):
        yield f'<p>🔍 Analisando Comprovante {i+1} com IA...</p>'
        
        texto_pg = page.extract_text()
        dados = analisar_com_gemini(texto_pg, "comprovante")
        
        # Delay visual e de segurança
        time.sleep(4)
        
        writer_temp = PdfWriter()
        writer_temp.add_page(page)
        pdf_bytes = io.BytesIO()
        writer_temp.write(pdf_bytes)
        pdf_bytes.seek(0)
        
        comprovantes_map.append({'page_obj': pdf_bytes, 'dados': dados, 'usado': False})

    # B. LER BOLETOS
    yield f'<hr><p>📂 Iniciando leitura dos Boletos...</p>'
    
    # Prepara o ZIP final em memória
    output_zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(output_zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for idx, boleto_path in enumerate(lista_caminhos_boletos):
            nome_arquivo = os.path.basename(boleto_path)
            yield f'<p>📄 Processando Boleto {idx+1}: {nome_arquivo}...</p>'
            
            temp_reader = PdfReader(boleto_path)
            total_paginas_contadas += len(temp_reader.pages)
            
            texto_boleto = extract_text_from_pdf(boleto_path)
            dados_boleto = analisar_com_gemini(texto_boleto, "boleto")
            time.sleep(4)

            # Match
            yield f'<span>... Buscando comprovante compatível (Valor: R$ {dados_boleto.get("valor")})... </span>'
            comprovante_match = None
            for comp in comprovantes_map:
                if not comp['usado']:
                    v1 = float(dados_boleto.get('valor') or 0)
                    v2 = float(comp['dados'].get('valor') or 0)
                    if v1 > 0 and abs(v1 - v2) < 0.05:
                        comprovante_match = comp
                        comp['usado'] = True
                        break
            
            status_msg = "✅ MATCH ENCONTRADO!" if comprovante_match else "❌ SEM COMPROVANTE"
            yield f'<strong>{status_msg}</strong><br>'

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

    # C. FINALIZAÇÃO E SALVAMENTO DO ZIP PÚBLICO
    yield f'<hr><p>💾 Gerando arquivo final...</p>'
    
    # Salva o ZIP numa pasta pública de downloads
    pasta_downloads = os.path.join(settings.MEDIA_ROOT, 'downloads')
    os.makedirs(pasta_downloads, exist_ok=True)
    
    nome_zip = f"Resultado_{uuid.uuid4().hex[:8]}.zip"
    caminho_zip_final = os.path.join(pasta_downloads, nome_zip)
    
    with open(caminho_zip_final, "wb") as f:
        f.write(output_zip_buffer.getvalue())
        
    # Atualiza usuário
    user.paginas_processadas += total_paginas_contadas
    user.save()
    
    url_download = f"{settings.MEDIA_URL}downloads/{nome_zip}"
    
    yield f"""
    <h1 style="color: #fff;">CONCILIAÇÃO CONCLUÍDA! 🏁</h1>
    <p>Total de páginas processadas: {total_paginas_contadas}</p>
    <br>
    <a href="{url_download}" style="background: yellow; color: black; padding: 15px 30px; text-decoration: none; font-size: 20px; border-radius: 5px;">
        ⬇️ BAIXAR ARQUIVO ZIP AGORA
    </a>
    <br><br>
    <a href="/tools/pdf/" style="color: white;">Voltar</a>
    </div>
    """

    def processar_conciliacao_json_stream(lista_caminhos_boletos, caminho_comprovantes, user):
    import json # Garantir import
    
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
            
            # Avisa o frontend que começou este arquivo
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
            
            # Avisa o resultado
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
    
    # Envia evento final com link
    yield send_event('finish', {'url': url_download, 'total': total_paginas_contadas})