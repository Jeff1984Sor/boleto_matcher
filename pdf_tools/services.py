import io
import os
import zipfile
import time
import uuid
import json
import re  # Importante para limpar os caracteres
from pypdf import PdfReader, PdfWriter
import google.generativeai as genai
from django.conf import settings

genai.configure(api_key=settings.GOOGLE_API_KEY)

# Função auxiliar para deixar apenas números (remove . - e espaços)
def limpar_apenas_numeros(texto):
    if not texto:
        return ""
    # \D significa "tudo que não é dígito (0-9)"
    return re.sub(r'\D', '', str(texto))

def extract_text_from_pdf(pdf_path):
    try:
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            extracted = page.extract_text()
            if extracted:
                text += extracted + "\n"
        return text
    except Exception as e:
        print(f"Erro ao ler PDF: {e}")
        return ""

def analisar_com_gemini(texto, tipo_doc):
    model = genai.GenerativeModel('gemini-2.0-flash')
    
    # Prompt ajustado para focar na LINHA DIGITÁVEL / CÓDIGO DE BARRAS
    prompt = f"""
    Você é um assistente financeiro. Analise o texto abaixo extraído de um {tipo_doc} (PDF).
    
    Sua tarefa é extrair:
    1. O valor total do documento.
    2. A linha digitável ou código de barras numérico completo (geralmente tem entre 44 a 48 dígitos). 
       Procure por sequências longas de números. No caso de comprovantes, procure onde diz "Código de barras".
    
    Retorne APENAS um objeto JSON válido (sem markdown ```json) com os campos:
    - "valor": (float, use ponto para decimais. Ex: 150.50. Se não achar, 0.0)
    - "codigo_barras": (string, apenas os números encontrados. Ex: "816900000042...")
    
    Texto do documento:
    {texto[:6000]}
    """
    
    for tentativa in range(1, 4):
        try:
            response = model.generate_content(prompt)
            # Limpeza básica caso a IA mande markdown
            clean_text = response.text.replace('```json', '').replace('```', '').strip()
            return json.loads(clean_text)
        except Exception as e:
            if "429" in str(e):
                time.sleep(5) # Espera um pouco se der erro de cota
            else:
                print(f"Erro na IA: {e}")
                break
                
    return {"valor": 0.0, "codigo_barras": ""}

def processar_conciliacao_json_stream(lista_caminhos_boletos, caminho_comprovantes, user):
    
    # Helper para enviar JSON no stream
    def send_event(type, data):
        return json.dumps({'type': type, 'data': data}) + "\n"

    yield send_event('log', '🚀 Iniciando IA para leitura inteligente...')
    
    total_paginas_contadas = 0
    comprovantes_map = []
    
    # ==========================================
    # A. LER COMPROVANTES (E extrair códigos)
    # ==========================================
    yield send_event('log', '📂 Lendo arquivo de Comprovantes...')
    reader_comp = PdfReader(caminho_comprovantes)
    total_paginas_contadas += len(reader_comp.pages)
    
    for i, page in enumerate(reader_comp.pages):
        yield send_event('comp_status', {'index': i, 'msg': f'Lendo pág {i+1}...'})
        
        texto_pg = page.extract_text()
        dados = analisar_com_gemini(texto_pg, "comprovante de pagamento")
        
        # Aqui fazemos a "mágica": limpamos o código recebido da IA
        codigo_limpo = limpar_apenas_numeros(dados.get('codigo_barras'))
        dados['codigo_limpo'] = codigo_limpo
        
        # Pausa pequena para não estourar a API do Google
        time.sleep(2)
        
        # Prepara a página isolada para mesclar depois
        writer_temp = PdfWriter()
        writer_temp.add_page(page)
        pdf_bytes = io.BytesIO()
        writer_temp.write(pdf_bytes)
        pdf_bytes.seek(0)
        
        comprovantes_map.append({
            'page_obj': pdf_bytes, 
            'dados': dados, 
            'usado': False,
            'index': i
        })
        
        print(f"Comprovante {i}: Valor {dados['valor']} | Cod: {codigo_limpo}")

    # ==========================================
    # B. LER BOLETOS E CRUZAR DADOS
    # ==========================================
    yield send_event('log', '📂 Processando Boletos individualmente...')
    output_zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(output_zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for idx, boleto_path in enumerate(lista_caminhos_boletos):
            nome_arquivo = os.path.basename(boleto_path)
            
            yield send_event('file_start', {'filename': nome_arquivo})
            
            temp_reader = PdfReader(boleto_path)
            total_paginas_contadas += len(temp_reader.pages)
            
            texto_boleto = extract_text_from_pdf(boleto_path)
            dados_boleto = analisar_com_gemini(texto_boleto, "boleto bancário/DAMSP")
            
            # Limpa o código do boleto
            codigo_boleto_limpo = limpar_apenas_numeros(dados_boleto.get('codigo_barras'))
            valor_boleto = float(dados_boleto.get('valor') or 0)
            
            print(f"Boleto {nome_arquivo}: Valor {valor_boleto} | Cod: {codigo_boleto_limpo}")
            time.sleep(2)

            # LÓGICA DE MATCH (CRUZAMENTO)
            comprovante_match = None
            
            # 1. Tentativa Principal: Match exato pelo Código de Barras (sem espaços/pontos)
            if codigo_boleto_limpo:
                for comp in comprovantes_map:
                    if not comp['usado'] and comp['dados']['codigo_limpo'] == codigo_boleto_limpo:
                        comprovante_match = comp
                        yield send_event('log', f'✅ Match por Código de Barras: {nome_arquivo}')
                        break
            
            # 2. Tentativa Secundária: Se não achou pelo código, tenta pelo Valor (fallback)
            if not comprovante_match and valor_boleto > 0:
                for comp in comprovantes_map:
                    if not comp['usado']:
                        valor_comp = float(comp['dados'].get('valor') or 0)
                        # Margem de erro de 5 centavos
                        if abs(valor_boleto - valor_comp) < 0.05:
                            comprovante_match = comp
                            yield send_event('log', f'⚠️ Match por Valor (código falhou): {nome_arquivo}')
                            break

            if comprovante_match:
                comprovante_match['usado'] = True
                match_status = 'success'
            else:
                match_status = 'warning'
                yield send_event('log', f'❌ Sem comprovante encontrado para: {nome_arquivo}')

            yield send_event('file_done', {'filename': nome_arquivo, 'status': match_status})

            # ==========================================
            # MONTAR O PDF FINAL (Boleto + Comprovante)
            # ==========================================
            writer_final = PdfWriter()
            
            # Adiciona as páginas do boleto original
            reader_bol = PdfReader(boleto_path)
            for p in reader_bol.pages:
                writer_final.add_page(p)
            
            # Se achou comprovante, adiciona a página dele
            if comprovante_match:
                reader_match = PdfReader(comprovante_match['page_obj'])
                # Assume que a página isolada no map é a correta
                writer_final.add_page(reader_match.pages[0])
            
            pdf_output = io.BytesIO()
            writer_final.write(pdf_output)
            
            # Salva dentro do ZIP
            zip_file.writestr(nome_arquivo, pdf_output.getvalue())

    # ==========================================
    # C. FINALIZA E SALVA
    # ==========================================
    yield send_event('log', '💾 Gerando arquivo final...')
    
    pasta_downloads = os.path.join(settings.MEDIA_ROOT, 'downloads')
    os.makedirs(pasta_downloads, exist_ok=True)
    
    nome_zip = f"Conciliacao_{uuid.uuid4().hex[:8]}.zip"
    caminho_zip_final = os.path.join(pasta_downloads, nome_zip)
    
    with open(caminho_zip_final, "wb") as f:
        f.write(output_zip_buffer.getvalue())
        
    # Atualiza contador do usuário (se existir o atributo)
    if hasattr(user, 'paginas_processadas'):
        user.paginas_processadas += total_paginas_contadas
        user.save()
    
    url_download = f"{settings.MEDIA_URL}downloads/{nome_zip}"
    
    yield send_event('finish', {'url': url_download, 'total': total_paginas_contadas})