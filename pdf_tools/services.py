import io
import os
import zipfile
import uuid
import json
import re
import logging
import time
import fitz  # PyMuPDF
from diflib import SequenceMatcher
from pypdf import PdfReader, PdfWriter
from PIL import Image
import google.generativeai as genai
from django.conf import settings

# Configuração do logger para registrar informações e erros.
logger = logging.getLogger(__name__)

# Configura a API do Google Gemini com a chave das configurações do Django.
genai.configure(api_key=settings.GOOGLE_API_KEY)

# ============================================================
# FERRAMENTAS AUXILIARES
# ============================================================

def limpar_numeros(texto):
    """Remove todos os caracteres não numéricos de uma string."""
    return re.sub(r'\D', '', str(texto or ""))

def calcular_similaridade(a, b):
    """Calcula a similaridade entre duas strings (útil para códigos de barras)."""
    if not a or not b: return 0.0
    return SequenceMatcher(None, a, b).ratio()

def normalizar_valor(v_str):
    """Converte uma string de valor monetário (ex: 'R$ 1.234,56') para float (1234.56)."""
    try:
        if isinstance(v_str, (float, int)): return float(v_str)
        v = str(v_str).replace('R$', '').strip()
        if ',' in v and '.' in v: v = v.replace('.', '').replace(',', '.')
        elif ',' in v: v = v.replace(',', '.')
        return float(v)
    except (ValueError, TypeError):
        return 0.0

def extrair_valor_nome(nome_arquivo):
    """Tenta extrair um valor monetário do nome do arquivo como um fallback."""
    match = re.search(r'R\$\s?(\d+)[_.,-](\d{2})', nome_arquivo)
    if match:
        try:
            return float(f"{match.group(1)}.{match.group(2)}")
        except: pass
    return 0.0

def pdf_bytes_para_imagem_pil(pdf_bytes):
    """Converte os bytes da primeira página de um PDF para uma imagem PIL de alta qualidade."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    page = doc[0]
    # Zoom de 2x para melhorar a qualidade da imagem, crucial para a precisão da IA.
    matriz_zoom = fitz.Matrix(2, 2)
    pix = page.get_pixmap(matrix=matriz_zoom)
    return Image.open(io.BytesIO(pix.tobytes("jpeg")))


# ============================================================
# FUNÇÕES DA IA GEMINI (EXTRAÇÃO RÁPIDA E DESEMPATE PROFUNDO)
# ============================================================

def chamar_gemini_extracao_rapida(imagem_pil, tipo_doc):
    """Usa o modelo FLASH para extração rápida de valor e código. (Etapa 1)"""
    model = genai.GenerativeModel('gemini-2.0-flash')
    prompt = f"""
    Analise esta imagem de um {tipo_doc}. Extraia o VALOR TOTAL e o CÓDIGO DE BARRAS numérico (linha digitável).
    Retorne APENAS um objeto JSON válido com as chaves "valor" (float) e "codigo" (string).
    Se um campo não for encontrado, use null.
    Exemplo: {{ "valor": 123.45, "codigo": "0019050095..." }}
    """
    for tentativa in range(3):
        try:
            response = model.generate_content([prompt, imagem_pil])
            texto_resposta = response.text.replace('```json', '').replace('```', '').strip()
            return json.loads(texto_resposta)
        except Exception as e:
            logger.error(f"Erro na extração rápida (tentativa {tentativa+1}): {e}")
            time.sleep(2 * (tentativa + 1)) # Aumenta o tempo de espera a cada falha
    return {}

def chamar_gemini_desempate(img_boleto, lista_imgs_comprovantes):
    """Usa o modelo PRO para uma análise profunda e decidir qual comprovante é o correto. (Etapa 2)"""
    logger.info(f"Acionando IA de desempate para {len(lista_imgs_comprovantes)} comprovantes.")
    model = genai.GenerativeModel('gemini-2.5-flash-lite') # O MODELO MAIS PODEROSO

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
        # Aumentamos o tempo de espera aqui, pois o modelo PRO é mais lento.
        response = model.generate_content(prompt_parts)
        texto_resposta = response.text.replace('```json', '').replace('```', '').strip()
        return json.loads(texto_resposta)
    except Exception as e:
        logger.error(f"Erro crítico na IA de desempate: {e}")
        return {"melhor_indice_candidato": -1, "justificativa": "Erro na IA de desempate."}


def extrair_dados_pdf_fitz(pdf_bytes, tipo_doc, nome_arquivo=""):
    """Função principal de extração que usa o modelo rápido."""
    try:
        imagem_pil = pdf_bytes_para_imagem_pil(pdf_bytes)
        dados_ia = chamar_gemini_extracao_rapida(imagem_pil, tipo_doc)
        
        resultado = {
            'codigo': limpar_numeros(dados_ia.get('codigo')),
            'valor': normalizar_valor(dados_ia.get('valor')),
            'origem': 'IA_GEMINI'
        }
        
        if resultado['valor'] == 0 and nome_arquivo:
            valor_nome = extrair_valor_nome(nome_arquivo)
            if valor_nome > 0:
                resultado['valor'] = valor_nome
                resultado['origem'] = 'NOME_ARQUIVO'
        
        return resultado
    except Exception as e:
        logger.error(f"Erro ao extrair dados do PDF '{nome_arquivo}': {e}")
        valor_nome = extrair_valor_nome(nome_arquivo)
        return {'codigo': '', 'valor': valor_nome, 'origem': 'ERRO_FATAL'}

# ============================================================
# FLUXO PRINCIPAL DA RECONCILIAÇÃO (LÓGICA MELHORADA)
# ============================================================

def processar_reconciliacao(caminho_comprovantes, lista_caminhos_boletos, user):
    def emit(tipo, dados):
        return json.dumps({'type': tipo, 'data': dados}) + "\n"
    
    yield emit('log', '🚀 Iniciando reconciliação com IA de 2 etapas...')

    # --- ETAPA 1: LER E PROCESSAR O PDF DE COMPROVANTES ---
    yield emit('log', '📸 Lendo Comprovantes (Etapa 1: Extração Rápida)...')
    pool_comprovantes = []
    
    try:
        doc_comprovantes = fitz.open(caminho_comprovantes)
        reader_zip = PdfReader(caminho_comprovantes)
        
        for i, page in enumerate(doc_comprovantes):
            # Gera imagem PIL para ser usada pela IA
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
            img_pil = Image.open(io.BytesIO(pix.tobytes("jpeg")))
            
            # Pausa estratégica para respeitar os limites da API
            time.sleep(1.5)
            dados_ia = chamar_gemini_extracao_rapida(img_pil, "comprovante bancário")
            
            valor = normalizar_valor(dados_ia.get('valor'))
            codigo = limpar_numeros(dados_ia.get('codigo'))
            
            # Prepara os bytes da página individual para o ZIP final.
            writer = PdfWriter(); writer.add_page(reader_zip.pages[i]); bio = io.BytesIO(); writer.write(bio)
            
            # Adiciona o comprovante à 'piscina', guardando também a imagem PIL para o desempate
            pool_comprovantes.append({
                'id': i, 'codigo': codigo, 'valor': valor,
                'pdf_bytes': bio.getvalue(), 'imagem_pil': img_pil, 'usado': False
            })
            
            codigo_curto = f"...{codigo[-6:]}" if codigo else "N/A"
            yield emit('log', f"   🧾 Comprovante Pág {i+1}: R${valor} | Cód: {codigo_curto}")
            yield emit('comp_status', {'index': i, 'msg': f"R$ {valor}"})

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
            dados_boleto = extrair_dados_pdf_fitz(pdf_bytes_boleto, "boleto bancário", nome_arquivo)
            
            boleto_atual = {
                'nome': nome_arquivo, 'codigo': dados_boleto['codigo'],
                'valor': dados_boleto['valor'], 'pdf_bytes': pdf_bytes_boleto,
                'match': None, 'motivo': 'Sem comprovante compatível'
            }
            
            if boleto_atual['valor'] > 0:
                # Filtra candidatos: mesmo valor (com margem de 5 centavos) e que não foram usados ainda
                candidatos = [c for c in pool_comprovantes if not c['usado'] and abs(c['valor'] - boleto_atual['valor']) < 0.05]
                
                if candidatos:
                    melhor_candidato = None
                    # --- NOVA LÓGICA DE DECISÃO ---
                    if len(candidatos) == 1:
                        # Se só há UM candidato, o caso está resolvido.
                        melhor_candidato = candidatos[0]
                        boleto_atual['motivo'] = "VALOR (Candidato Único)"
                    else: # Múltiplos candidatos, precisamos investigar mais a fundo
                        # 1. Tentativa por similaridade de código de barras
                        maior_score = 0.0
                        possivel_melhor_por_codigo = None
                        for c in candidatos:
                            score = calcular_similaridade(boleto_atual['codigo'], c['codigo'])
                            if score > maior_score:
                                maior_score = score
                                possivel_melhor_por_codigo = c
                        
                        if maior_score > 0.65: # Se similaridade for alta, confia no código.
                            melhor_candidato = possivel_melhor_por_codigo
                            boleto_atual['motivo'] = f"CÓDIGO ({int(maior_score*100)}%)"
                        else:
                            # 2. AMBIGUIDADE DETECTADA -> ACIONAR DESEMPATE COM IA PROFUNDA
                            yield emit('log', f"   🔍 Ambiguidade em R${boleto_atual['valor']}. Acionando IA de análise profunda...")
                            img_boleto = pdf_bytes_para_imagem_pil(boleto_atual['pdf_bytes'])
                            imgs_comprovantes_candidatos = [c['imagem_pil'] for c in candidatos]
                            
                            # Chamada para a IA mais poderosa
                            resultado_desempate = chamar_gemini_desempate(img_boleto, imgs_comprovantes_candidatos)
                            
                            indice_escolhido = resultado_desempate.get('melhor_indice_candidato', -1)
                            justificativa = resultado_desempate.get('justificativa', 'IA não encontrou par.')
                            
                            if indice_escolhido >= 0 and indice_escolhido < len(candidatos):
                                # A IA escolheu um candidato com sucesso!
                                melhor_candidato = candidatos[indice_escolhido]
                                boleto_atual['motivo'] = f"IA PROFUNDA ({justificativa})"
                            else:
                                # Se a IA não conseguiu decidir, voltamos ao FIFO para não parar o processo.
                                melhor_candidato = candidatos[0] # Pega o primeiro da lista de candidatos
                                boleto_atual['motivo'] = "VALOR (IA indecisa, usando Fila)"

                    if melhor_candidato:
                        boleto_atual['match'] = melhor_candidato
                        melhor_candidato['usado'] = True # Marca o comprovante como usado para não ser pego de novo
            
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
            # Adiciona o boleto original
            writer.append(io.BytesIO(boleto['pdf_bytes']))
            # Se encontrou um par, adiciona o comprovante logo em seguida
            if boleto['match']:
                writer.append(io.BytesIO(boleto['match']['pdf_bytes']))
            
            # Salva o PDF combinado (1 ou 2 páginas) em memória
            pdf_combinado_bytes = io.BytesIO()
            writer.write(pdf_combinado_bytes)
            
            # Adiciona o PDF combinado ao arquivo ZIP com o nome do boleto original
            zip_file.writestr(boleto['nome'], pdf_combinado_bytes.getvalue())

    # Salva o arquivo ZIP em disco na pasta de downloads da mídia
    pasta_destino = os.path.join(settings.MEDIA_ROOT, 'downloads')
    os.makedirs(pasta_destino, exist_ok=True)
    nome_zip = f"Conciliacao_Final_{uuid.uuid4().hex[:8]}.zip"
    caminho_completo_zip = os.path.join(pasta_destino, nome_zip)
    
    with open(caminho_completo_zip, 'wb') as f:
        f.write(output_zip.getvalue())
        
    url_download = f"{settings.MEDIA_URL}downloads/{nome_zip}"
    yield emit('finish', {'url': url_download, 'total': len(lista_final_boletos)})