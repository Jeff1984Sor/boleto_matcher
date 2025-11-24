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

logger = logging.getLogger(__name__)
genai.configure(api_key=settings.GOOGLE_API_KEY)

# ============================================================
# FERRAMENTAS
# ============================================================

def limpar_numeros(texto):
    return re.sub(r'\D', '', str(texto or ""))

def calcular_similaridade(a, b):
    if not a or not b: return 0.0
    return SequenceMatcher(None, a, b).ratio()

def normalizar_valor(v_str):
    try:
        if isinstance(v_str, (float, int)): return float(v_str)
        v = str(v_str).replace('R$', '').strip()
        if ',' in v and '.' in v: v = v.replace('.', '').replace(',', '.')
        elif ',' in v: v = v.replace(',', '.')
        return float(v)
    except: return 0.0

def extrair_valor_nome(nome):
    match = re.search(r'R\$\s?(\d+)[_.,-](\d{2})', nome)
    if match:
        try: return float(f"{match.group(1)}.{match.group(2)}")
        except: pass
    return 0.0

# ============================================================
# GEMINI 2.0 (COM RETRY E SLEEP)
# ============================================================

def chamar_gemini_imagem(imagem_pil, tipo_doc):
    model = genai.GenerativeModel('gemini-2.0-flash-exp')
    prompt = f"""
    Analise esta imagem de {tipo_doc}.
    ATENÇÃO: O documento pode estar rotacionado ou com qualidade ruim.
    
    TAREFA:
    1. Encontre o VALOR TOTAL do pagamento (R$).
    2. Encontre o CÓDIGO DE BARRAS numérico (Linha Digitável).
       - Copie TODOS os números.
    
    Retorne JSON: {{ "valor": 0.00, "codigo": "string" }}
    """
    
    # Tenta 3 vezes em caso de erro 429 (Rate Limit) ou erro de servidor
    for tentativa in range(3):
        try:
            response = model.generate_content([prompt, imagem_pil])
            txt = response.text.replace('```json', '').replace('```', '').strip()
            return json.loads(txt)
        except Exception as e:
            if "429" in str(e): # Muita requisição
                time.sleep(4)
            else:
                time.sleep(1)
    return {}

def extrair_dados_pdf_fitz(pdf_bytes, tipo_doc, nome_arquivo=""):
    """
    Usa Fitz para gerar imagem e Gemini para ler.
    """
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        page = doc[0] # 1ª página
        # Zoom de 2x para melhorar qualidade da imagem (ajuda no código de barras)
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2)) 
        img_data = pix.tobytes("jpeg")
        imagem_pil = Image.open(io.BytesIO(img_data))
        
        dados = chamar_gemini_imagem(imagem_pil, tipo_doc)
        
        res = {
            'codigo': limpar_numeros(dados.get('codigo')),
            'valor': normalizar_valor(dados.get('valor')),
            'origem': 'GEMINI'
        }
        
        # Fallback nome
        if res['valor'] == 0 and nome_arquivo:
            v = extrair_valor_nome(nome_arquivo)
            if v > 0:
                res['valor'] = v
                res['origem'] = 'NOME_ARQ'
        
        return res

    except Exception as e:
        logger.error(f"Erro extração: {e}")
        v = extrair_valor_nome(nome_arquivo)
        return {'codigo': '', 'valor': v, 'origem': 'ERRO'}

# ============================================================
# FLUXO PRINCIPAL
# ============================================================

def processar_reconciliacao(caminho_comprovantes, lista_caminhos_boletos, user):
    
    def emit(tipo, dados):
        return json.dumps({'type': tipo, 'data': dados}) + "\n"
    
    yield emit('log', '🚀 Iniciando Gemini 2.0 (Modo Lento e Preciso)...')

    # --- 1. LER COMPROVANTES (UM A UM) ---
    yield emit('log', '📸 Lendo Comprovantes (Isso pode demorar um pouco)...')
    pool_comprovantes = []
    
    try:
        # Abre o PDF Original com Fitz para iterar
        doc_orig = fitz.open(caminho_comprovantes)
        total_pags = len(doc_orig)
        
        # Abre com PyPDF apenas para extrair os bytes para o ZIP final
        reader_zip = PdfReader(caminho_comprovantes)
        
        for i in range(total_pags):
            # 1. Gera Imagem da Página
            page = doc_orig[i]
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
            img_data = pix.tobytes("jpeg")
            img_pil = Image.open(io.BytesIO(img_data))
            
            # 2. Chama Gemini (COM PAUSA)
            time.sleep(1.5) # <--- O SEGREDO: Respeita o limite da API
            dados = chamar_gemini_imagem(img_pil, "comprovante bancário")
            
            val = normalizar_valor(dados.get('valor'))
            cod = limpar_numeros(dados.get('codigo'))
            
            # 3. Prepara bytes para o ZIP final
            writer = PdfWriter()
            writer.add_page(reader_zip.pages[i])
            bio = io.BytesIO()
            writer.write(bio)
            bytes_pdf = bio.getvalue()
            
            item = {
                'id': i,
                'codigo': cod,
                'valor': val,
                'pdf_bytes': bytes_pdf,
                'usado': False
            }
            pool_comprovantes.append(item)
            
            show = f"...{cod[-6:]}" if cod else "FALHA"
            origem = "IA" if val > 0 else "ILEGIVEL"
            
            yield emit('comp_status', {'index': i, 'msg': f"R${val} ({origem})"})
            yield emit('log', f"   🧾 Pág {i+1}: R${val} | {show}")

    except Exception as e:
        yield emit('log', f"❌ Erro crítico comprovantes: {e}")
        return

    # --- 2. LER BOLETOS ---
    yield emit('log', '⚡ Analisando Boletos...')
    lista_final = []

    for path in lista_caminhos_boletos:
        nome = os.path.basename(path)
        yield emit('file_start', {'filename': nome})
        
        try:
            with open(path, 'rb') as f: pdf_bytes = f.read()
            
            # Chama IA para o Boleto
            time.sleep(1) # Pausa para respirar
            d = extrair_dados_pdf_fitz(pdf_bytes, "boleto bancário", nome)
            
            boleto = {
                'nome': nome,
                'codigo': d['codigo'],
                'valor': d['valor'],
                'pdf_bytes': pdf_bytes,
                'match': None,
                'motivo': ''
            }
            
            # === MATCH (LÓGICA FILA) ===
            match_ok = False
            melhor = None
            maior_score = 0.0
            
            if boleto['valor'] > 0:
                # Filtra candidatos disponiveis com mesmo valor
                candidatos = [c for c in pool_comprovantes if not c['usado'] and abs(c['valor'] - boleto['valor']) < 0.05]
                
                if candidatos:
                    # 1. Tenta achar código parecido
                    for cand in candidatos:
                        score = calcular_similaridade(boleto['codigo'], cand['codigo'])
                        if score > maior_score:
                            maior_score = score
                            melhor = cand
                    
                    # 2. Decisão
                    aceito = False
                    
                    if maior_score > 0.6:
                        aceito = True
                        boleto['motivo'] = f"CÓDIGO ({int(maior_score*100)}%)"
                    elif boleto['codigo'] == "" and len(candidatos) > 0:
                        # Se boleto não leu código, pega o primeiro da fila
                        melhor = candidatos[0] # FIFO
                        aceito = True
                        boleto['motivo'] = "VALOR (Boleto s/ Cod)"
                    elif len(candidatos) == 1:
                        # Só tem um valor igual sobrando, é ele
                        melhor = candidatos[0]
                        aceito = True
                        boleto['motivo'] = "VALOR (Único)"
                    elif len(candidatos) > 0:
                        # Varios valores iguais, códigos diferentes.
                        # Assume o primeiro da fila (FIFO) para não perder o match
                        # Isso resolve o caso de 8 boletos de 402 reais iguais
                        melhor = candidatos[0]
                        aceito = True
                        boleto['motivo'] = "VALOR (Fila Sequencial)"

                    if aceito and melhor:
                        boleto['match'] = melhor
                        melhor['usado'] = True
                        match_ok = True
            
            if match_ok:
                yield emit('log', f"   ✅ {nome} -> {boleto['motivo']}")
                yield emit('file_done', {'filename': nome, 'status': 'success'})
            else:
                yield emit('log', f"   ❌ {nome} (R${boleto['valor']}) -> Sem par")
                yield emit('file_done', {'filename': nome, 'status': 'warning'})
                
            lista_final.append(boleto)

        except Exception as e:
            yield emit('log', f"⚠️ Erro {nome}: {e}")

    # --- 3. ZIP ---
    yield emit('log', '💾 Gerando Zip...')
    output_zip = io.BytesIO()
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for item in lista_final:
            w = PdfWriter()
            w.append(io.BytesIO(item['pdf_bytes']))
            if item['match']:
                w.append(io.BytesIO(item['match']['pdf_bytes']))
            bio = io.BytesIO()
            w.write(bio)
            zip_file.writestr(item['nome'], bio.getvalue())

    pasta = os.path.join(settings.MEDIA_ROOT, 'downloads')
    os.makedirs(pasta, exist_ok=True)
    nome_zip = f"Conciliacao_Final_{uuid.uuid4().hex[:8]}.zip"
    with open(os.path.join(pasta, nome_zip), 'wb') as f: f.write(output_zip.getvalue())
        
    yield emit('finish', {'url': f"{settings.MEDIA_URL}downloads/{nome_zip}", 'total': len(lista_final)})