import io
import os
import zipfile
import uuid
import json
import re
import logging
import time
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
    Esta função é a chave para a nova lógica de conciliação.
    """
    model = genai.GenerativeModel('gemini-1.5-flash')
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
            response = model.generate_content([prompt, imagem_pil])
            texto_resposta = response.text.strip()
            # Tratamento para garantir que o JSON seja limpo
            if texto_resposta.startswith("```json"):
                texto_resposta = texto_resposta[7:]
            if texto_resposta.endswith("```"):
                texto_resposta = texto_resposta[:-3]
            
            return json.loads(texto_resposta.strip())
        except (json.JSONDecodeError, Exception) as e:
            logger.error(f"Erro na extração estruturada (tentativa {tentativa+1}): {e}")
            time.sleep(2 * (tentativa + 1))
    return {} # Retorna um dicionário vazio em caso de falha total

# ============================================================
# FUNÇÕES DO FLUXO PRINCIPAL (ATUALIZADAS)
# ============================================================

def processar_pagina(pdf_bytes, tipo_doc, nome_arquivo=""):
    """
    Função principal para processar uma única página de PDF, agora usando a extração estruturada.
    """
    try:
        imagem_pil = pdf_bytes_para_imagem_pil(pdf_bytes)
        # Chama a nova função de extração
        dados_ia = extrair_dados_estruturados_com_ia(imagem_pil, tipo_doc)
        
        # Garante que os campos essenciais existam
        resultado = {
            'codigo': limpar_numeros(dados_ia.get('codigo_barras_numerico')),
            'valor': normalizar_valor(dados_ia.get('valor_float')),
            'dados_completos': dados_ia, # Armazena o JSON completo
            'origem': 'IA_GEMINI_ESTRUTURADO'
        }
        
        # Fallback para o nome do arquivo se o valor não for extraído pela IA
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
    """Usa o modelo PRO para uma análise profunda e decidir qual comprovante é o correto. (Etapa 2)"""
    logger.info(f"Acionando IA de desempate para {len(lista_imgs_comprovantes)} comprovantes.")
    model = genai.GenerativeModel('gemini-1.5-pro') # O MODELO MAIS PODEROSO

    # Monta a requisição com todas as imagens, devidamente legendadas.
    prompt_parts = [
        "Você é um analista financeiro especialista em conciliação. Sua tarefa é resolver uma ambiguidade.",
        "A seguir, apresento UMA imagem de BOLETO e VÁRIAS imagens de COMPROVANTES de pagamento que possuem o mesmo valor.",
        "Analise TODOS os detalhes visuais (data de vencimento vs data de pagamento, nome do beneficiário, nome do pagador, CNPJ/CPF, número do documento, etc.) para encontrar o par PERFEITO.",
        "\n--- IMAGEM DO BOLETO PARA ANÁLISE ---",
        img_boleto,
        "\n--- IMAGENS DOS COMPROVANTES CANDIDATOS ---",
    ]
    for i, img_comp in enumerate(lista_imgs_comprovantes):
        prompt_parts.append(f"\nCANDIDATO ÍNDICE {i}:")
        prompt_parts.append(img_comp)

    prompt_parts.append("""
    Com base na sua análise detalhada, retorne um objeto JSON com o índice do melhor comprovante candidato.
    O índice deve corresponder à ordem que os candidatos foram apresentados (começando em 0).
    Se NENHUM deles parecer uma combinação confiável, retorne o índice -1.

    Formato de saída OBRIGATÓRIO:
    { "melhor_indice_candidato": <numero>, "justificativa": "<sua análise concisa aqui>" }
    """)

    try:
        response = model.generate_content(prompt_parts)
        texto_resposta = response.text.replace('```json', '').replace('```', '').strip()
        return json.loads(texto_resposta)
    except Exception as e:
        logger.error(f"Erro crítico na IA de desempate: {e}")
        return {"melhor_indice_candidato": -1, "justificativa": "Erro na IA de desempate."}

# ============================================================
# FLUXO PRINCIPAL DA RECONCILIAÇÃO (LÓGICA ATUALIZADA)
# ============================================================

def processar_reconciliacao(caminho_comprovantes, lista_caminhos_boletos, user):
    def emit(tipo, dados):
        return json.dumps({'type': tipo, 'data': dados}) + "\n"
    
    yield emit('log', '🚀 Iniciando reconciliação com extração estruturada...')

    # --- ETAPA 1: LER E PROCESSAR O PDF DE COMPROVANTES ---
    yield emit('log', '📸 Lendo Comprovantes (Etapa 1: Extração Estruturada)...')
    pool_comprovantes = []
    
    try:
        doc_comprovantes = fitz.open(caminho_comprovantes)
        reader_zip = PdfReader(caminho_comprovantes)
        
        for i, page in enumerate(doc_comprovantes):
            writer = PdfWriter(); writer.add_page(reader_zip.pages[i]); bio = io.BytesIO(); writer.write(bio)
            pdf_bytes = bio.getvalue()

            # Pausa estratégica para respeitar os limites da API
            time.sleep(1.5)
            # Usa a nova função de processamento
            dados_pagina = processar_pagina(pdf_bytes, "comprovante bancário")
            
            # Adiciona o comprovante à 'piscina', agora com dados estruturados
            pool_comprovantes.append({
                'id': i,
                'codigo': dados_pagina['codigo'],
                'valor': dados_pagina['valor'],
                'dados_completos': dados_pagina['dados_completos'],
                'pdf_bytes': pdf_bytes,
                'usado': False
            })
            
            codigo_curto = f"...{dados_pagina['codigo'][-6:]}" if dados_pagina['codigo'] else "N/A"
            yield emit('log', f"   🧾 Comprovante Pág {i+1}: R${dados_pagina['valor']} | Cód: {codigo_curto}")
            yield emit('comp_status', {'index': i, 'msg': f"R$ {dados_pagina['valor']}"})

    except Exception as e:
        yield emit('log', f"❌ Erro crítico ao ler comprovantes: {e}"); return

    # --- ETAPA 2: LER OS BOLETOS E APLICAR LÓGICA DE MATCH AVANÇADA ---
    yield emit('log', '⚡ Analisando Boletos e combinando com comprovantes...')
    lista_final_boletos = []

    for path_boleto in lista_caminhos_boletos:
        nome_arquivo = os.path.basename(path_boleto)
        yield emit('file_start', {'filename': nome_arquivo})
        
        try:
            with open(path_boleto, 'rb') as f: pdf_bytes_boleto = f.read()
            
            time.sleep(1) # Pausa
            # Usa a nova função de processamento para o boleto
            dados_boleto = processar_pagina(pdf_bytes_boleto, "boleto bancário", nome_arquivo)
            
            boleto_atual = {
                'nome': nome_arquivo,
                'codigo': dados_boleto['codigo'],
                'valor': dados_boleto['valor'],
                'dados_completos': dados_boleto['dados_completos'],
                'pdf_bytes': pdf_bytes_boleto,
                'match': None,
                'motivo': 'Sem comprovante compatível'
            }
            
            if boleto_atual['valor'] > 0:
                # Filtro inicial por valor
                candidatos = [c for c in pool_comprovantes if not c['usado'] and abs(c['valor'] - boleto_atual['valor']) < 0.05]
                
                if candidatos:
                    melhor_candidato = None
                    
                    # 1. Tentativa de match por CÓDIGO DE BARRAS (o mais confiável)
                    if boleto_atual['codigo']:
                        for c in candidatos:
                            if c['codigo'] and calcular_similaridade(boleto_atual['codigo'], c['codigo']) > 0.95:
                                melhor_candidato = c
                                boleto_atual['motivo'] = "CÓDIGO DE BARRAS"
                                break
                    
                    # 2. Se não houver match por código, e só há um candidato, assume ele.
                    if not melhor_candidato and len(candidatos) == 1:
                        melhor_candidato = candidatos[0]
                        boleto_atual['motivo'] = "VALOR (Candidato Único)"
                    
                    # 3. Se ainda há ambiguidade (múltiplos candidatos), usa a IA de desempate
                    elif not melhor_candidato and len(candidatos) > 1:
                        yield emit('log', f"   🔍 Ambiguidade em R${boleto_atual['valor']}. Acionando IA de análise profunda...")
                        img_boleto = pdf_bytes_para_imagem_pil(boleto_atual['pdf_bytes'])
                        # Precisamos das imagens dos candidatos para a IA de desempate
                        imgs_comprovantes_candidatos = [pdf_bytes_para_imagem_pil(c['pdf_bytes']) for c in candidatos]
                        
                        resultado_desempate = chamar_gemini_desempate(img_boleto, imgs_comprovantes_candidatos)
                        indice_escolhido = resultado_desempate.get('melhor_indice_candidato', -1)
                        justificativa = resultado_desempate.get('justificativa', 'IA não encontrou par.')
                        
                        if indice_escolhido != -1:
                            melhor_candidato = candidatos[indice_escolhido]
                            boleto_atual['motivo'] = f"IA PROFUNDA ({justificativa})"
                        else:
                            # Fallback final: FIFO
                            melhor_candidato = candidatos[0]
                            boleto_atual['motivo'] = "VALOR (IA indecisa, usando Fila)"

                    if melhor_candidato:
                        boleto_atual['match'] = melhor_candidato
                        melhor_candidato['usado'] = True

            if boleto_atual['match']:
                yield emit('log', f"   ✅ {nome_arquivo} -> Combinado por {boleto_atual['motivo']}")
                yield emit('file_done', {'filename': nome_arquivo, 'status': 'success'})
            else:
                yield emit('log', f"   ⚠️ {nome_arquivo} (R${boleto_atual['valor']}) -> Não encontrado")
                yield emit('file_done', {'filename': nome_arquivo, 'status': 'warning'})
                
            lista_final_boletos.append(boleto_atual)

        except Exception as e:
            yield emit('log', f"❌ Erro no arquivo {nome_arquivo}: {e}")

    # --- ETAPA 3: GERAR O ARQUIVO ZIP DE SAÍDA ---
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

    # Salva o arquivo ZIP
    pasta_destino = os.path.join(settings.MEDIA_ROOT, 'downloads')
    os.makedirs(pasta_destino, exist_ok=True)
    nome_zip = f"Conciliacao_Final_{uuid.uuid4().hex[:8]}.zip"
    caminho_completo_zip = os.path.join(pasta_destino, nome_zip)
    
    with open(caminho_completo_zip, 'wb') as f:
        f.write(output_zip.getvalue())
        
    url_download = f"{settings.MEDIA_URL}downloads/{nome_zip}"
    yield emit('finish', {'url': url_download, 'total': len(lista_final_boletos)})
