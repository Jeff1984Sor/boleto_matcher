import io
import os
import zipfile
import uuid
import json
import re
import logging
import time
import concurrent.futures
import fitz  # PyMuPDF
from pypdf import PdfReader, PdfWriter
from PIL import Image
import google.generativeai as genai
from django.conf import settings

# ConfiguraÃ§Ã£o do logger
logger = logging.getLogger(__name__)

# Configura a API do Google Gemini
genai.configure(api_key=settings.GOOGLE_API_KEY)

# ============================================================
# FERRAMENTAS AUXILIARES
# ============================================================

def gerar_conteudo_com_timeout(model, parts, timeout_s):
    """Executa generate_content com timeout para evitar travar o stream."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(model.generate_content, parts)
        try:
            return future.result(timeout=timeout_s)
        except concurrent.futures.TimeoutError:
            logger.error(f"Timeout na chamada do Gemini ({timeout_s}s).")
            return None

def limpar_numeros(texto):
    """Remove todos os caracteres nÃ£o numÃ©ricos de uma string."""
    return re.sub(r'\D', '', str(texto or ""))

def linha_digitavel_bancaria_para_codigo(linha):
    """Converte linha digitavel bancaria (47) em codigo de barras (44)."""
    if not linha or len(linha) != 47 or not linha.isdigit():
        return ""
    return f"{linha[0:4]}{linha[32]}{linha[33:37]}{linha[37:47]}{linha[4:9]}{linha[10:20]}{linha[21:31]}"

def linha_digitavel_arrecadacao_para_codigo(linha):
    """Converte linha digitavel de arrecadacao (48) em codigo de barras (44)."""
    if not linha or len(linha) != 48 or not linha.isdigit():
        return ""
    return f"{linha[0:11]}{linha[12:23]}{linha[24:35]}{linha[36:47]}"

def normalizar_codigo_barras(codigo):
    """
    Normaliza o codigo para formato comparavel.
    Remove pontos/tracos e converte linha digitavel para codigo de barras.
    """
    somente_numeros = limpar_numeros(codigo)
    if not somente_numeros:
        return ""
    if len(somente_numeros) == 44:
        return somente_numeros
    if len(somente_numeros) == 47:
        return linha_digitavel_bancaria_para_codigo(somente_numeros)
    if len(somente_numeros) == 48:
        return linha_digitavel_arrecadacao_para_codigo(somente_numeros)
    return ""

def codigos_sao_iguais(codigo_a, codigo_b):
    """Compara codigos apos normalizacao."""
    a = normalizar_codigo_barras(codigo_a)
    b = normalizar_codigo_barras(codigo_b)
    return bool(a and b and a == b)

def normalizar_valor(v_str):
    """Converte uma string de valor monetÃ¡rio para float."""
    try:
        if isinstance(v_str, (float, int)): return float(v_str)
        v = str(v_str).replace('R$', '').strip()
        if ',' in v and '.' in v: v = v.replace('.', '').replace(',', '.')
        elif ',' in v: v = v.replace(',', '.')
        return float(v)
    except (ValueError, TypeError):
        return 0.0

def extrair_valor_nome(nome_arquivo):
    """Tenta extrair um valor monetÃ¡rio do nome do arquivo."""
    match = re.search(r'R\$\s?(\d+)[_.,-](\d{2})', nome_arquivo)
    if match:
        try:
            return float(f"{match.group(1)}.{match.group(2)}")
        except: pass
    return 0.0

def pdf_bytes_para_imagem_pil(pdf_bytes):
    """Converte a primeira pÃ¡gina de um PDF em uma imagem PIL de alta qualidade."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    page = doc[0]
    matriz_zoom = fitz.Matrix(2, 2)
    pix = page.get_pixmap(matrix=matriz_zoom)
    return Image.open(io.BytesIO(pix.tobytes("jpeg")))

# ============================================================
# NOVA FUNÃ‡ÃƒO DE EXTRAÃ‡ÃƒO ESTRUTURADA COM IA
# ============================================================

def extrair_dados_estruturados_com_ia(imagem_pil, tipo_doc):
    """
    Usa um modelo de IA para extrair um JSON estruturado de uma imagem de documento.
    """
    model = genai.GenerativeModel(settings.GEMINI_MODEL)
    timeout_s = int(os.getenv('GEMINI_TIMEOUT_SECONDS', '300'))
    prompt = f"""
    Analise esta imagem de um {tipo_doc}. Sua tarefa Ã© extrair as seguintes informaÃ§Ãµes
    e retornÃ¡-las em um objeto JSON VÃLIDO.

    1.  `codigo_barras_numerico`: A linha digitÃ¡vel ou cÃ³digo de barras, contendo APENAS NÃšMEROS.
    2.  `data_vencimento`: A data de vencimento do boleto (formato YYYY-MM-DD). Se nÃ£o houver, use null.
    3.  `data_pagamento`: A data em que o pagamento foi efetuado (formato YYYY-MM-DD). Se nÃ£o houver, use null.
    4.  `valor_float`: O valor principal do documento como um nÃºmero float (ex: 123.45).
    5.  `valor_virgula`: O mesmo valor, mas como uma string com vÃ­rgula (ex: "123,45").
    6.  `nome_beneficiario`: O nome da empresa ou pessoa que recebe o dinheiro.
    7.  `nome_pagador`: O nome da empresa ou pessoa que estÃ¡ pagando.
    8.  `cnpj_beneficiario`: O CNPJ do beneficiÃ¡rio.
    9.  `cnpj_pagador`: O CNPJ do pagador.

    REGRAS IMPORTANTES:
    - Se um campo nÃ£o for encontrado, seu valor DEVE ser `null`.
    - O JSON de saÃ­da nÃ£o deve conter nenhum caractere de formataÃ§Ã£o como ```json ou ```.
    - Preste muita atenÃ§Ã£o para diferenciar beneficiÃ¡rio de pagador.

    Exemplo de SaÃ­da:
    {{
      "codigo_barras_numerico": "34191790010352013781368109400000187220000015000",
      "data_vencimento": "2024-07-31",
      "data_pagamento": "2024-07-30",
      "valor_float": 150.00,
      "valor_virgula": "150,00",
      "nome_beneficiario": "MINHA EMPRESA LTDA",
      "nome_pagador": "CLIENTE EXEMPLO SA",
      "cnpj_beneficiario": "12.345.678/0001-99",
      "cnpj_pagador": "98.765.432/0001-11"
    }}
    """
    for tentativa in range(3):
        try:
            response = gerar_conteudo_com_timeout(model, [prompt, imagem_pil], timeout_s)
            if response is None:
                time.sleep(2 * (tentativa + 1))
                continue
            texto_resposta = response.text.strip()
            if texto_resposta.startswith("```json"):
                texto_resposta = texto_resposta[7:]
            if texto_resposta.endswith("```"):
                texto_resposta = texto_resposta[:-3]
            return json.loads(texto_resposta.strip())
        except (json.JSONDecodeError, Exception) as e:
            logger.error(f"Erro na extraÃ§Ã£o estruturada (tentativa {tentativa+1}): {e}")
            time.sleep(2 * (tentativa + 1))
    return {}

# ============================================================
# FUNÃ‡Ã•ES DO FLUXO PRINCIPAL (ATUALIZADAS)
# ============================================================

def processar_pagina(pdf_bytes, tipo_doc, nome_arquivo=""):
    """
    Processa uma pÃ¡gina de PDF, usando a extraÃ§Ã£o estruturada.
    """
    try:
        imagem_pil = pdf_bytes_para_imagem_pil(pdf_bytes)
        dados_ia = extrair_dados_estruturados_com_ia(imagem_pil, tipo_doc)
        
        resultado = {
            'codigo': normalizar_codigo_barras(dados_ia.get('codigo_barras_numerico')),
            'valor': normalizar_valor(dados_ia.get('valor_float')),
            'dados_completos': dados_ia,
            'origem': 'IA_GEMINI_ESTRUTURADO'
        }
        
        if resultado['valor'] == 0 and nome_arquivo:
            valor_nome = extrair_valor_nome(nome_arquivo)
            if valor_nome > 0:
                resultado['valor'] = valor_nome
                resultado['origem'] = 'NOME_ARQUIVO'
        
        return resultado
    except Exception as e:
        logger.error(f"Erro ao processar pÃ¡gina do PDF '{nome_arquivo}': {e}")
        valor_nome = extrair_valor_nome(nome_arquivo)
        return {'codigo': '', 'valor': valor_nome, 'dados_completos': {}, 'origem': 'ERRO_FATAL'}

def chamar_gemini_desempate(img_boleto, lista_imgs_comprovantes):
    """Usa IA para anÃ¡lise profunda e desempate."""
    logger.info(f"Acionando IA de desempate para {len(lista_imgs_comprovantes)} comprovantes.")
    model = genai.GenerativeModel(settings.GEMINI_MODEL)
    timeout_s = int(os.getenv('GEMINI_TIMEOUT_SECONDS', '300'))
    prompt_parts = [
        "VocÃª Ã© um analista financeiro. Sua tarefa Ã© resolver uma ambiguidade.",
        "Apresento UMA imagem de BOLETO e VÃRIAS de COMPROVANTES com o mesmo valor.",
        "Analise todos os detalhes para encontrar o par PERFEITO (datas, nomes, CNPJ, etc.).",
        "\n--- BOLETO ---", img_boleto, "\n--- COMPROVANTES CANDIDATOS ---",
    ]
    for i, img_comp in enumerate(lista_imgs_comprovantes):
        prompt_parts.extend([f"\nCANDIDATO ÃNDICE {i}:", img_comp])
    prompt_parts.append("""
    Retorne um JSON com `melhor_indice_candidato` (o Ã­ndice do melhor comprovante, ou -1 se nenhum for confiÃ¡vel)
    e uma `justificativa` concisa.
    Formato: { "melhor_indice_candidato": <numero>, "justificativa": "<sua anÃ¡lise>" }
    """)
    try:
        response = gerar_conteudo_com_timeout(model, prompt_parts, timeout_s)
        if response is None:
            return {"melhor_indice_candidato": -1, "justificativa": "Timeout na IA."}
        texto_resposta = response.text.replace('```json', '').replace('```', '').strip()
        return json.loads(texto_resposta)
    except Exception as e:
        logger.error(f"Erro crÃ­tico na IA de desempate: {e}")
        return {"melhor_indice_candidato": -1, "justificativa": "Erro na IA."}

# ============================================================
# FLUXO PRINCIPAL DA RECONCILIAÃ‡ÃƒO (LÃ“GICA ATUALIZADA)
# ============================================================

def processar_reconciliacao(caminho_comprovantes, lista_caminhos_boletos, user):
    def emit(tipo, dados):
        return json.dumps({'type': tipo, 'data': dados}) + "\n"
    
    # FunÃ§Ã£o auxiliar para formatar o log detalhado
    def formatar_log_extracao(dados, tipo, identificador):
        d = dados['dados_completos']
        valor = dados.get('valor', 0.0)
        data = d.get('data_pagamento') or d.get('data_vencimento') or 'N/A'
        pagador = d.get('nome_pagador', 'N/A')
        beneficiario = d.get('nome_beneficiario', 'N/A')
        codigo = dados.get('codigo', 'N/A')
        return (
            f"   -> {tipo} {identificador} | R${valor:.2f} | Data: {data} | "
            f"Pagador: {pagador} | BeneficiÃ¡rio: {beneficiario} | CÃ³d: {codigo}"
        )

    yield emit('log', 'ðŸš€ Iniciando reconciliaÃ§Ã£o com extraÃ§Ã£o estruturada...')

    # --- ETAPA 1: LER COMPROVANTES ---
    yield emit('log', 'ðŸ“¸ Lendo Comprovantes...')
    pool_comprovantes = []
    try:
        doc_comprovantes = fitz.open(caminho_comprovantes)
        reader_zip = PdfReader(caminho_comprovantes)
        for i, page in enumerate(doc_comprovantes):
            writer = PdfWriter(); writer.add_page(reader_zip.pages[i]); bio = io.BytesIO(); writer.write(bio)
            pdf_bytes = bio.getvalue()
            time.sleep(1.5)
            dados_pagina = processar_pagina(pdf_bytes, "comprovante bancÃ¡rio")
            pool_comprovantes.append({
                'id': i, **dados_pagina,
                'pdf_bytes': pdf_bytes, 'usado': False
            })
            yield emit('log', formatar_log_extracao(dados_pagina, "Comprovante", f"PÃ¡g {i+1}"))
            yield emit('comp_status', {'index': i, 'msg': f"R$ {dados_pagina['valor']:.2f}"})
    except Exception as e:
        yield emit('log', f"âŒ Erro crÃ­tico ao ler comprovantes: {e}"); return

    # --- ETAPA 2: LER BOLETOS E COMBINAR ---
    yield emit('log', 'âš¡ Analisando Boletos e combinando...')
    lista_final_boletos = []
    for path_boleto in lista_caminhos_boletos:
        nome_arquivo = os.path.basename(path_boleto)
        yield emit('file_start', {'filename': nome_arquivo})
        try:
            with open(path_boleto, 'rb') as f: pdf_bytes_boleto = f.read()
            time.sleep(1)
            dados_boleto = processar_pagina(pdf_bytes_boleto, "boleto bancÃ¡rio", nome_arquivo)
            yield emit('log', formatar_log_extracao(dados_boleto, "Boleto", f'({nome_arquivo})'))

            boleto_atual = {
                'nome': nome_arquivo, **dados_boleto,
                'pdf_bytes': pdf_bytes_boleto, 'match': None,
                'motivo': 'Sem comprovante compatÃ­vel'
            }
            
            if boleto_atual['codigo']:
                candidatos_codigo = [
                    c for c in pool_comprovantes
                    if not c['usado'] and c['codigo'] and codigos_sao_iguais(boleto_atual['codigo'], c['codigo'])
                ]
                if len(candidatos_codigo) == 1:
                    boleto_atual['match'] = candidatos_codigo[0]
                    boleto_atual['motivo'] = "CODIGO DE BARRAS (COMPLETO)"
                    boleto_atual['match']['usado'] = True
                elif len(candidatos_codigo) > 1 and boleto_atual['valor'] > 0:
                    candidatos_codigo_valor = [c for c in candidatos_codigo if abs(c['valor'] - boleto_atual['valor']) < 0.05]
                    if len(candidatos_codigo_valor) == 1:
                        boleto_atual['match'] = candidatos_codigo_valor[0]
                        boleto_atual['motivo'] = "CODIGO DE BARRAS (COMPLETO) + VALOR"
                        boleto_atual['match']['usado'] = True

            if not boleto_atual['match'] and boleto_atual['valor'] > 0:
                candidatos = [c for c in pool_comprovantes if not c['usado'] and abs(c['valor'] - boleto_atual['valor']) < 0.05]
                if candidatos:
                    melhor_candidato = None
                    if boleto_atual['codigo']:
                        for c in candidatos:
                            if c['codigo'] and codigos_sao_iguais(boleto_atual['codigo'], c['codigo']):
                                melhor_candidato = c
                                boleto_atual['motivo'] = "CODIGO DE BARRAS (COMPLETO)"
                                break
                    if not melhor_candidato and len(candidatos) == 1:
                        melhor_candidato = candidatos[0]
                        boleto_atual['motivo'] = "VALOR (Candidato Unico)"
                    elif not melhor_candidato and len(candidatos) > 1:
                        yield emit('log', f"   - Ambiguidade em R${boleto_atual['valor']:.2f}. Acionando IA de analise profunda...")
                        img_boleto = pdf_bytes_para_imagem_pil(boleto_atual['pdf_bytes'])
                        imgs_comprovantes_candidatos = [pdf_bytes_para_imagem_pil(c['pdf_bytes']) for c in candidatos]
                        resultado_desempate = chamar_gemini_desempate(img_boleto, imgs_comprovantes_candidatos)
                        indice_escolhido = resultado_desempate.get('melhor_indice_candidato', -1)
                        if indice_escolhido != -1:
                            melhor_candidato = candidatos[indice_escolhido]
                            boleto_atual['motivo'] = f"IA PROFUNDA ({resultado_desempate.get('justificativa')})"
                        else:
                            melhor_candidato = candidatos[0]
                            boleto_atual['motivo'] = "VALOR (IA indecisa, usando Fila)"
                    if melhor_candidato:
                        boleto_atual['match'] = melhor_candidato
                        melhor_candidato['usado'] = True

            if boleto_atual['match']:
                yield emit('log', f"   âœ… COMBINADO: {nome_arquivo} -> Comprovante PÃ¡g {boleto_atual['match']['id']+1} (Motivo: {boleto_atual['motivo']})")
                yield emit('file_done', {'filename': nome_arquivo, 'status': 'success'})
            else:
                yield emit('log', f"   âš ï¸ NÃƒO COMBINADO: {nome_arquivo}")
                yield emit('file_done', {'filename': nome_arquivo, 'status': 'warning'})
            lista_final_boletos.append(boleto_atual)
        except Exception as e:
            yield emit('log', f"âŒ Erro no arquivo {nome_arquivo}: {e}")

    # --- ETAPA 3: GERAR ZIP ---
    yield emit('log', 'ðŸ’¾ Montando o arquivo ZIP final...')
    output_zip = io.BytesIO()
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for boleto in lista_final_boletos:
            writer = PdfWriter()
            writer.append(io.BytesIO(boleto['pdf_bytes']))
            if boleto['match']:
                writer.append(io.BytesIO(boleto['match']['pdf_bytes']))
            pdf_combinado_bytes = io.BytesIO()
            writer.write(pdf_combinado_bytes)
            zip_file.writestr(boleto['nome'], pdf_combinado_bytes.getvalue())

    pasta_destino = os.path.join(settings.MEDIA_ROOT, 'downloads')
    os.makedirs(pasta_destino, exist_ok=True)
    nome_zip = f"Conciliacao_Final_{uuid.uuid4().hex[:8]}.zip"
    caminho_completo_zip = os.path.join(pasta_destino, nome_zip)
    with open(caminho_completo_zip, 'wb') as f:
        f.write(output_zip.getvalue())
    url_download = f"{settings.MEDIA_URL}downloads/{nome_zip}"
    yield emit('finish', {'url': url_download, 'total': len(lista_final_boletos)})
