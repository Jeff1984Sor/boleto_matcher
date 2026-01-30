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
from difflib import SequenceMatcher
from pypdf import PdfReader, PdfWriter
from PIL import Image
import google.generativeai as genai
from django.conf import settings

# Configuração do logger
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
    """Remove todos os caracteres não numéricos de uma string."""
    return re.sub(r'\D', '', str(texto or ""))

def calcular_similaridade(a, b):
    """Calcula a similaridade entre duas strings."""
    if not a or not b: return 0.0
    return SequenceMatcher(None, a, b).ratio()

def normalizar_valor(v_str):
    """Converte uma string de valor monetário para float."""
    try:
        if isinstance(v_str, (float, int)): return float(v_str)
        v = str(v_str).replace('R$', '').strip()
        if ',' in v and '.' in v: v = v.replace('.', '').replace(',', '.')
        elif ',' in v: v = v.replace(',', '.')
        return float(v)
    except (ValueError, TypeError):
        return 0.0

def extrair_valor_nome(nome_arquivo):
    """Tenta extrair um valor monetário do nome do arquivo."""
    match = re.search(r'R\$\s?(\d+)[_.,-](\d{2})', nome_arquivo)
    if match:
        try:
            return float(f"{match.group(1)}.{match.group(2)}")
        except: pass
    return 0.0

def pdf_bytes_para_imagem_pil(pdf_bytes):
    """Converte a primeira página de um PDF em uma imagem PIL de alta qualidade."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    page = doc[0]
    matriz_zoom = fitz.Matrix(2, 2)
    pix = page.get_pixmap(matrix=matriz_zoom)
    return Image.open(io.BytesIO(pix.tobytes("jpeg")))

# ============================================================
# NOVA FUNÇÃO DE EXTRAÇÃO ESTRUTURADA COM IA
# ============================================================

def extrair_dados_estruturados_com_ia(imagem_pil, tipo_doc):
    """
    Usa um modelo de IA para extrair um JSON estruturado de uma imagem de documento.
    """
    model = genai.GenerativeModel('gemini-2.5-pro')
    timeout_s = int(os.getenv('GEMINI_TIMEOUT_SECONDS', '300'))
    prompt = f"""
    Analise esta imagem de um {tipo_doc}. Sua tarefa é extrair as seguintes informações
    e retorná-las em um objeto JSON VÁLIDO.

    1.  `codigo_barras_numerico`: A linha digitável ou código de barras, contendo APENAS NÚMEROS.
    2.  `data_vencimento`: A data de vencimento do boleto (formato YYYY-MM-DD). Se não houver, use null.
    3.  `data_pagamento`: A data em que o pagamento foi efetuado (formato YYYY-MM-DD). Se não houver, use null.
    4.  `valor_float`: O valor principal do documento como um número float (ex: 123.45).
    5.  `valor_virgula`: O mesmo valor, mas como uma string com vírgula (ex: "123,45").
    6.  `nome_beneficiario`: O nome da empresa ou pessoa que recebe o dinheiro.
    7.  `nome_pagador`: O nome da empresa ou pessoa que está pagando.
    8.  `cnpj_beneficiario`: O CNPJ do beneficiário.
    9.  `cnpj_pagador`: O CNPJ do pagador.

    REGRAS IMPORTANTES:
    - Se um campo não for encontrado, seu valor DEVE ser `null`.
    - O JSON de saída não deve conter nenhum caractere de formatação como ```json ou ```.
    - Preste muita atenção para diferenciar beneficiário de pagador.

    Exemplo de Saída:
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
            logger.error(f"Erro na extração estruturada (tentativa {tentativa+1}): {e}")
            time.sleep(2 * (tentativa + 1))
    return {}

# ============================================================
# FUNÇÕES DO FLUXO PRINCIPAL (ATUALIZADAS)
# ============================================================

def processar_pagina(pdf_bytes, tipo_doc, nome_arquivo=""):
    """
    Processa uma página de PDF, usando a extração estruturada.
    """
    try:
        imagem_pil = pdf_bytes_para_imagem_pil(pdf_bytes)
        dados_ia = extrair_dados_estruturados_com_ia(imagem_pil, tipo_doc)
        
        resultado = {
            'codigo': limpar_numeros(dados_ia.get('codigo_barras_numerico')),
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
        logger.error(f"Erro ao processar página do PDF '{nome_arquivo}': {e}")
        valor_nome = extrair_valor_nome(nome_arquivo)
        return {'codigo': '', 'valor': valor_nome, 'dados_completos': {}, 'origem': 'ERRO_FATAL'}

def chamar_gemini_desempate(img_boleto, lista_imgs_comprovantes):
    """Usa IA para análise profunda e desempate."""
    logger.info(f"Acionando IA de desempate para {len(lista_imgs_comprovantes)} comprovantes.")
    model = genai.GenerativeModel('gemini-2.5-pro')
    timeout_s = int(os.getenv('GEMINI_TIMEOUT_SECONDS', '300'))
    prompt_parts = [
        "Você é um analista financeiro. Sua tarefa é resolver uma ambiguidade.",
        "Apresento UMA imagem de BOLETO e VÁRIAS de COMPROVANTES com o mesmo valor.",
        "Analise todos os detalhes para encontrar o par PERFEITO (datas, nomes, CNPJ, etc.).",
        "\n--- BOLETO ---", img_boleto, "\n--- COMPROVANTES CANDIDATOS ---",
    ]
    for i, img_comp in enumerate(lista_imgs_comprovantes):
        prompt_parts.extend([f"\nCANDIDATO ÍNDICE {i}:", img_comp])
    prompt_parts.append("""
    Retorne um JSON com `melhor_indice_candidato` (o índice do melhor comprovante, ou -1 se nenhum for confiável)
    e uma `justificativa` concisa.
    Formato: { "melhor_indice_candidato": <numero>, "justificativa": "<sua análise>" }
    """)
    try:
        response = gerar_conteudo_com_timeout(model, prompt_parts, timeout_s)
        if response is None:
            return {"melhor_indice_candidato": -1, "justificativa": "Timeout na IA."}
        texto_resposta = response.text.replace('```json', '').replace('```', '').strip()
        return json.loads(texto_resposta)
    except Exception as e:
        logger.error(f"Erro crítico na IA de desempate: {e}")
        return {"melhor_indice_candidato": -1, "justificativa": "Erro na IA."}

# ============================================================
# FLUXO PRINCIPAL DA RECONCILIAÇÃO (LÓGICA ATUALIZADA)
# ============================================================

def processar_reconciliacao(caminho_comprovantes, lista_caminhos_boletos, user):
    def emit(tipo, dados):
        return json.dumps({'type': tipo, 'data': dados}) + "\n"
    
    # Função auxiliar para formatar o log detalhado
    def formatar_log_extracao(dados, tipo, identificador):
        d = dados['dados_completos']
        valor = dados.get('valor', 0.0)
        data = d.get('data_pagamento') or d.get('data_vencimento') or 'N/A'
        pagador = d.get('nome_pagador', 'N/A')
        beneficiario = d.get('nome_beneficiario', 'N/A')
        codigo = dados.get('codigo', 'N/A')
        return (
            f"   -> {tipo} {identificador} | R${valor:.2f} | Data: {data} | "
            f"Pagador: {pagador} | Beneficiário: {beneficiario} | Cód: {codigo}"
        )

    yield emit('log', '🚀 Iniciando reconciliação com extração estruturada...')

    # --- ETAPA 1: LER COMPROVANTES ---
    yield emit('log', '📸 Lendo Comprovantes...')
    pool_comprovantes = []
    try:
        doc_comprovantes = fitz.open(caminho_comprovantes)
        reader_zip = PdfReader(caminho_comprovantes)
        for i, page in enumerate(doc_comprovantes):
            writer = PdfWriter(); writer.add_page(reader_zip.pages[i]); bio = io.BytesIO(); writer.write(bio)
            pdf_bytes = bio.getvalue()
            time.sleep(1.5)
            dados_pagina = processar_pagina(pdf_bytes, "comprovante bancário")
            pool_comprovantes.append({
                'id': i, **dados_pagina,
                'pdf_bytes': pdf_bytes, 'usado': False
            })
            yield emit('log', formatar_log_extracao(dados_pagina, "Comprovante", f"Pág {i+1}"))
            yield emit('comp_status', {'index': i, 'msg': f"R$ {dados_pagina['valor']:.2f}"})
    except Exception as e:
        yield emit('log', f"❌ Erro crítico ao ler comprovantes: {e}"); return

    # --- ETAPA 2: LER BOLETOS E COMBINAR ---
    yield emit('log', '⚡ Analisando Boletos e combinando...')
    lista_final_boletos = []
    for path_boleto in lista_caminhos_boletos:
        nome_arquivo = os.path.basename(path_boleto)
        yield emit('file_start', {'filename': nome_arquivo})
        try:
            with open(path_boleto, 'rb') as f: pdf_bytes_boleto = f.read()
            time.sleep(1)
            dados_boleto = processar_pagina(pdf_bytes_boleto, "boleto bancário", nome_arquivo)
            yield emit('log', formatar_log_extracao(dados_boleto, "Boleto", f'({nome_arquivo})'))

            boleto_atual = {
                'nome': nome_arquivo, **dados_boleto,
                'pdf_bytes': pdf_bytes_boleto, 'match': None,
                'motivo': 'Sem comprovante compatível'
            }
            
            if boleto_atual['valor'] > 0:
                candidatos = [c for c in pool_comprovantes if not c['usado'] and abs(c['valor'] - boleto_atual['valor']) < 0.05]
                if candidatos:
                    melhor_candidato = None
                    if boleto_atual['codigo']:
                        for c in candidatos:
                            if c['codigo'] and calcular_similaridade(boleto_atual['codigo'], c['codigo']) > 0.95:
                                melhor_candidato = c
                                boleto_atual['motivo'] = "CÓDIGO DE BARRAS"
                                break
                    if not melhor_candidato and len(candidatos) == 1:
                        melhor_candidato = candidatos[0]
                        boleto_atual['motivo'] = "VALOR (Candidato Único)"
                    elif not melhor_candidato and len(candidatos) > 1:
                        yield emit('log', f"   - Ambiguidade em R${boleto_atual['valor']:.2f}. Acionando IA de análise profunda...")
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
                yield emit('log', f"   ✅ COMBINADO: {nome_arquivo} -> Comprovante Pág {boleto_atual['match']['id']+1} (Motivo: {boleto_atual['motivo']})")
                yield emit('file_done', {'filename': nome_arquivo, 'status': 'success'})
            else:
                yield emit('log', f"   ⚠️ NÃO COMBINADO: {nome_arquivo}")
                yield emit('file_done', {'filename': nome_arquivo, 'status': 'warning'})
            lista_final_boletos.append(boleto_atual)
        except Exception as e:
            yield emit('log', f"❌ Erro no arquivo {nome_arquivo}: {e}")

    # --- ETAPA 3: GERAR ZIP ---
    yield emit('log', '💾 Montando o arquivo ZIP final...')
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
