import io
import os
import zipfile
import time
import uuid
import json
import re
from pypdf import PdfReader, PdfWriter
import google.generativeai as genai
from django.conf import settings

genai.configure(api_key=settings.GOOGLE_API_KEY)

def limpar_apenas_numeros(texto):
    """Remove tudo que não for número."""
    if not texto:
        return ""
    return re.sub(r'\D', '', str(texto))

def tentar_extrair_via_regex(texto):
    """
    Tenta achar sequências longas de números (típicas de boletos) 
    sem gastar IA. Remove espaços e quebras de linha antes de procurar.
    Boletos bancários = 47 digitos (linha digitavel) ou 44 (código de barras)
    DAMSP/Impostos = 48 digitos
    """
    texto_limpo = texto.replace('\n', '').replace(' ', '').replace('.', '').replace('-', '')
    # Procura sequência de 44 a 48 dígitos
    match = re.search(r'\d{44,48}', texto_limpo)
    if match:
        return match.group(0)
    return None

def chamar_gemini_com_fallback(texto, tipo_doc):
    """
    Lógica de Retentativa Inteligente:
    1. Tenta extrair valor e código com o modelo RÁPIDO (Flash).
    2. Verifica se o código de barras parece válido (tem > 40 numeros).
    3. Se não for válido, tenta com o modelo POTENTE (Pro).
    """
    
    # --- DEFINIÇÃO DO PROMPT ---
    prompt_base = f"""
    Analise o texto deste {tipo_doc}.
    Extraia:
    1. O valor total (float).
    2. O Código de Barras ou Linha Digitável (sequência numérica longa).
       - Boletos comuns têm ~47 dígitos.
       - Impostos/DAMSP (iniciados com 8) têm ~48 dígitos.
       - Ignore espaços, pontos e traços, quero apenas os NÚMEROS.
    
    Retorne JSON puro:
    {{
        "valor": 150.00,
        "codigo_barras": "816200000049..."
    }}
    
    Texto:
    {texto[:6000]}
    """

    # --- 1ª PASSADA: MODELO FLASH (Rápido e Barato) ---
    dados_extraidos = _executar_gemini(prompt_base, 'gemini-2.0-flash')
    
    codigo_limpo = limpar_apenas_numeros(dados_extraidos.get('codigo_barras'))
    
    # Critério de falha: Código vazio ou muito curto (boletos reais têm pelo menos 44 digitos)
    if len(codigo_limpo) < 44:
        print(f"⚠️ Flash falhou no código ({len(codigo_limpo)} dígitos). Tentando PRO...")
        
        # --- 2ª PASSADA: MODELO PRO (Mais caro, mas mais capaz) ---
        dados_pro = _executar_gemini(prompt_base, 'gemini-2.5-pro')
        
        # Se o Pro achou algo melhor, usamos ele
        codigo_pro_limpo = limpar_apenas_numeros(dados_pro.get('codigo_barras'))
        if len(codigo_pro_limpo) >= 44:
            return dados_pro
            
    return dados_extraidos

def _executar_gemini(prompt, model_name):
    """Função interna para chamar a API e tratar erros"""
    model = genai.GenerativeModel(model_name)
    for tentativa in range(1, 4):
        try:
            response = model.generate_content(prompt)
            clean_text = response.text.replace('```json', '').replace('```', '').strip()
            return json.loads(clean_text)
        except Exception as e:
            if "429" in str(e): # Too Many Requests
                time.sleep(5)
            else:
                print(f"Erro na IA ({model_name}): {e}")
                break
    return {"valor": 0.0, "codigo_barras": ""}

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
        print(f"Erro leitura PDF: {e}")
        return ""

def processar_conciliacao_json_stream(lista_caminhos_boletos, caminho_comprovantes, user):
    
    def send_event(type, data):
        return json.dumps({'type': type, 'data': data}) + "\n"

    yield send_event('log', '🚀 Iniciando processamento inteligente...')
    
    total_paginas_contadas = 0
    comprovantes_map = []
    
    # =================================================================
    # A. LER COMPROVANTES (Com Regex + Flash + Fallback Pro)
    # =================================================================
    yield send_event('log', '📂 Lendo Comprovantes (Modo Alta Precisão)...')
    reader_comp = PdfReader(caminho_comprovantes)
    total_paginas_contadas += len(reader_comp.pages)
    
    for i, page in enumerate(reader_comp.pages):
        yield send_event('comp_status', {'index': i, 'msg': f'Analisando pg {i+1}...'})
        
        texto_pg = page.extract_text() or ""
        
        # 1. Tenta Regex primeiro (Custo Zero e 100% preciso se o texto existir)
        codigo_regex = tentar_extrair_via_regex(texto_pg)
        
        if codigo_regex:
            # Se achou no regex, pede pra IA só pegar o VALOR pra economizar tokens/tempo
            # Ou chama a IA completa mas já sabemos que temos o código
            dados = chamar_gemini_com_fallback(texto_pg, "comprovante de pagamento")
            dados['codigo_barras'] = codigo_regex # Força o uso do Regex que é mais confiavel
            origem_cod = "REGEX"
        else:
            # Se não achou regex, vai para o fluxo Flash -> Pro
            dados = chamar_gemini_com_fallback(texto_pg, "comprovante de pagamento")
            origem_cod = "IA"

        codigo_limpo = limpar_apenas_numeros(dados.get('codigo_barras'))
        dados['codigo_limpo'] = codigo_limpo
        
        print(f"Comp {i+1}: Valor {dados.get('valor')} | Cod: {codigo_limpo[:10]}... ({len(codigo_limpo)} dig) [{origem_cod}]")
        
        # Prepara PDF unitário
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
        time.sleep(1) # Evitar rate limit

    # =================================================================
    # B. LER BOLETOS
    # =================================================================
    yield send_event('log', '📂 Processando Boletos...')
    output_zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(output_zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for boleto_path in lista_caminhos_boletos:
            nome_arquivo = os.path.basename(boleto_path)
            yield send_event('file_start', {'filename': nome_arquivo})
            
            temp_reader = PdfReader(boleto_path)
            total_paginas_contadas += len(temp_reader.pages)
            
            texto_boleto = extract_text_from_pdf(boleto_path)
            
            # Mesma lógica: Regex -> Flash -> Pro
            codigo_regex_bol = tentar_extrair_via_regex(texto_boleto)
            
            if codigo_regex_bol:
                dados_boleto = chamar_gemini_com_fallback(texto_boleto, "boleto bancário")
                dados_boleto['codigo_barras'] = codigo_regex_bol
                origem_bol = "REGEX"
            else:
                dados_boleto = chamar_gemini_com_fallback(texto_boleto, "boleto bancário")
                origem_bol = "IA"
            
            cod_boleto_limpo = limpar_apenas_numeros(dados_boleto.get('codigo_barras'))
            valor_boleto = float(dados_boleto.get('valor') or 0)
            
            print(f"Boleto {nome_arquivo}: Cod {cod_boleto_limpo[:10]}... [{origem_bol}]")

            # --- MATCHING ---
            comprovante_match = None
            
            # 1. Match Exato de Código
            if len(cod_boleto_limpo) > 40:
                for comp in comprovantes_map:
                    if not comp['usado']:
                        cod_comp = comp['dados']['codigo_limpo']
                        # Verifica se um contém o outro (para resolver casos onde um tem checksum e o outro não)
                        if cod_boleto_limpo == cod_comp:
                            comprovante_match = comp
                            break
                        # Fallback: Às vezes o boleto tem 48 digitos e o comprovante 44 (sem digito verificador geral)
                        # Vamos verificar se os primeiros 20 digitos batem, já é um match fortissimo
                        elif len(cod_comp) > 20 and cod_boleto_limpo.startswith(cod_comp[:20]):
                             comprovante_match = comp
                             break
            
            # 2. Match por Valor (Plano B)
            if not comprovante_match and valor_boleto > 0:
                for comp in comprovantes_map:
                    if not comp['usado']:
                        v_comp = float(comp['dados'].get('valor') or 0)
                        if abs(valor_boleto - v_comp) < 0.05:
                            comprovante_match = comp
                            yield send_event('log', f'⚠️ Match por VALOR em {nome_arquivo}')
                            break
            
            # Salva resultado
            status = 'warning'
            writer_final = PdfWriter()
            
            # Páginas do Boleto
            reader_bol = PdfReader(boleto_path)
            for p in reader_bol.pages:
                writer_final.add_page(p)
                
            if comprovante_match:
                status = 'success'
                comprovante_match['usado'] = True
                # Página do Comprovante
                reader_match = PdfReader(comprovante_match['page_obj'])
                writer_final.add_page(reader_match.pages[0])
            
            yield send_event('file_done', {'filename': nome_arquivo, 'status': status})
            
            pdf_out = io.BytesIO()
            writer_final.write(pdf_out)
            zip_file.writestr(nome_arquivo, pdf_out.getvalue())

    # C. Finaliza
    yield send_event('log', '💾 Compactando arquivos...')
    pasta_downloads = os.path.join(settings.MEDIA_ROOT, 'downloads')
    os.makedirs(pasta_downloads, exist_ok=True)
    
    nome_zip = f"Conciliacao_{uuid.uuid4().hex[:8]}.zip"
    caminho_zip = os.path.join(pasta_downloads, nome_zip)
    
    with open(caminho_zip, "wb") as f:
        f.write(output_zip_buffer.getvalue())
        
    if hasattr(user, 'paginas_processadas'):
        user.paginas_processadas += total_paginas_contadas
        user.save()
        
    yield send_event('finish', {'url': f"{settings.MEDIA_URL}downloads/{nome_zip}", 'total': total_paginas_contadas})