import io
import os
import zipfile
import uuid
import json
import re
import logging
import fitz  # PyMuPDF (Não precisa de poppler/linux)
from difflib import SequenceMatcher
from pypdf import PdfReader, PdfWriter
from PIL import Image
import google.generativeai as genai
from django.conf import settings

# Logger
logger = logging.getLogger(__name__)

# Configura API
genai.configure(api_key=settings.GOOGLE_API_KEY)

# ============================================================
# FERRAMENTAS
# ============================================================

def limpar_numeros(texto):
    """Deixa só dígitos."""
    return re.sub(r'\D', '', str(texto or ""))

def calcular_similaridade(a, b):
    """Retorna % de semelhança (0.0 a 1.0)."""
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
    """Lê '402_00' do nome do arquivo."""
    match = re.search(r'R\$\s?(\d+)[_.,-](\d{2})', nome)
    if match:
        try: return float(f"{match.group(1)}.{match.group(2)}")
        except: pass
    return 0.0

# ============================================================
# GEMINI 2.0 VISION (O MAIS PODEROSO)
# ============================================================

def extrair_com_gemini_2_0(pdf_bytes, tipo_doc, nome_arquivo=""):
    """
    Usa PyMuPDF para gerar imagem e Gemini 2.0 para ler.
    """
    # Tenta usar o modelo mais novo. Se der erro de acesso, usa o 1.5
    modelo_nome = 'gemini-2.0-flash-exp' 
    
    try:
        # 1. Converter PDF para Imagem (RAM)
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        page = doc.load_page(0) # Pega a 1ª página
        pix = page.get_pixmap(dpi=200) # 200 DPI é ótimo para OCR
        img_data = pix.tobytes("jpeg")
        imagem_pil = Image.open(io.BytesIO(img_data))
        
        # 2. Configura o Modelo
        model = genai.GenerativeModel(modelo_nome)
        
        prompt = f"""
        Analise esta imagem de {tipo_doc}.
        
        TAREFA:
        1. Identifique o VALOR TOTAL (R$).
        2. Identifique a LINHA DIGITÁVEL ou CÓDIGO DE BARRAS (Sequência numérica longa).
           - Para boletos comuns: ~47 dígitos.
           - Para impostos/prefeitura (DAMSP): ~48 dígitos (começa com 8).
           - Ignore espaços e pontos, extraia apenas os NÚMEROS.
        
        Responda APENAS este JSON:
        {{ "valor": 0.00, "codigo": "string_numerica" }}
        """

        # 3. Chama a IA
        response = model.generate_content([prompt, imagem_pil])
        texto_resp = response.text.replace('```json', '').replace('```', '').strip()
        dados = json.loads(texto_resp)
        
        res = {
            'codigo': limpar_numeros(dados.get('codigo')),
            'valor': normalizar_valor(dados.get('valor')),
            'origem': 'GEMINI_2.0'
        }
        
        # Fallback: Se a IA não achou valor, tenta o nome do arquivo
        if res['valor'] == 0 and nome_arquivo:
            v = extrair_valor_nome(nome_arquivo)
            if v > 0:
                res['valor'] = v
                res['origem'] += '+NOME'
                
        return res

    except Exception as e:
        print(f"Erro Gemini: {e}")
        # Último recurso: só o valor do nome
        v = extrair_valor_nome(nome_arquivo)
        return {'codigo': '', 'valor': v, 'origem': 'FALHA_IA'}

# ============================================================
# FLUXO DE PROCESSAMENTO
# ============================================================

def processar_reconciliacao(caminho_comprovantes, lista_caminhos_boletos, user):
    
    def emit(tipo, dados):
        return json.dumps({'type': tipo, 'data': dados}) + "\n"
    
    yield emit('log', '🚀 Iniciando Gemini 2.0 Vision (Alta Precisão)...')

    # --- 1. INVENTÁRIO DE COMPROVANTES ---
    yield emit('log', '📸 Analisando Comprovantes...')
    pool_comprovantes = []
    
    try:
        # Abre o PDFzão para cortar as páginas
        reader_pdf = PdfReader(caminho_comprovantes)
        
        # Precisamos abrir com Fitz também para gerar as imagens
        doc_fitz = fitz.open(caminho_comprovantes)
        
        for i, page in enumerate(reader_pdf.pages):
            # Extrai PDF (para salvar depois)
            writer = PdfWriter()
            writer.add_page(page)
            bio_pdf = io.BytesIO()
            writer.write(bio_pdf)
            bytes_pdf = bio_pdf.getvalue()
            
            # Gera imagem da página para a IA (usando Fitz direto do arquivo é mais rápido)
            pix = doc_fitz[i].get_pixmap(dpi=200)
            img_bytes = pix.tobytes("jpeg")
            
            # Chama IA passando a imagem crua (simulando o fluxo da função acima)
            # Vou chamar a função adaptada para aceitar bytes de imagem direto se quiser otimizar,
            # mas vamos usar o fluxo padrão passando o PDF bytes para manter consistência.
            d = extrair_com_gemini_2_0(bytes_pdf, "comprovante bancário")
            
            item = {
                'id': i,
                'codigo': d['codigo'],
                'valor': d['valor'],
                'pdf_bytes': bytes_pdf,
                'usado': False
            }
            pool_comprovantes.append(item)
            
            # Log
            cod_show = f"...{item['codigo'][-6:]}" if item['codigo'] else "SEM_COD"
            yield emit('comp_status', {'index': i, 'msg': f"R${item['valor']} ({cod_show})"})
            yield emit('log', f"   🧾 Pág {i+1}: R${item['valor']} | {cod_show}")

    except Exception as e:
        yield emit('log', f"❌ Erro crítico: {e}")
        return

    # --- 2. BOLETOS E MATCH ---
    yield emit('log', '⚡ Analisando Boletos...')
    lista_final = []

    for path in lista_caminhos_boletos:
        nome = os.path.basename(path)
        yield emit('file_start', {'filename': nome})
        
        try:
            with open(path, 'rb') as f: pdf_bytes = f.read()
            
            # Chama Gemini 2.0
            d = extrair_com_gemini_2_0(pdf_bytes, "boleto/guia de imposto", nome)
            
            boleto = {
                'nome': nome,
                'codigo': d['codigo'],
                'valor': d['valor'],
                'pdf_bytes': pdf_bytes,
                'match': None,
                'motivo': ''
            }
            
            # === LÓGICA DE MATCH INTELIGENTE ===
            match_encontrado = False
            melhor_cand = None
            maior_similiaridade = 0.0
            
            if boleto['valor'] > 0:
                # 1. Filtra candidatos pelo VALOR (Margem 0.05)
                # Só olha comprovantes não usados
                candidatos = [c for c in pool_comprovantes if not c['usado'] and abs(c['valor'] - boleto['valor']) < 0.05]
                
                if candidatos:
                    # 2. Dentre os de mesmo valor, calcula similaridade do código
                    for cand in candidatos:
                        simil = calcular_similaridade(boleto['codigo'], cand['codigo'])
                        
                        if simil > maior_similiaridade:
                            maior_similiaridade = simil
                            melhor_cand = cand
                    
                    # 3. Regras de Decisão
                    aceitar = False
                    motivo = ""
                    
                    if maior_similiaridade > 0.65: # 65% parecido (Resolve o prefixo igual)
                        aceitar = True
                        motivo = f"SIMILARIDADE {int(maior_similiaridade*100)}%"
                        
                    elif len(candidatos) == 1: 
                        # Código ruim ou diferente, mas é o ÚNICO valor disponivel
                        aceitar = True
                        motivo = "VALOR (Único na fila)"
                        
                    elif boleto['codigo'] == "" and len(candidatos) > 0:
                        # Boleto não leu código, pega o primeiro da fila de valor
                        # (Melhor arriscar do que deixar sem nada, já que o valor bate)
                        melhor_cand = candidatos[0]
                        aceitar = True
                        motivo = "VALOR (Fila - Boleto Ilegível)"

                    if aceitar and melhor_cand:
                        boleto['match'] = melhor_cand
                        melhor_cand['usado'] = True # Marca como usado!
                        boleto['motivo'] = motivo
                        match_encontrado = True
            
            if match_encontrado:
                yield emit('log', f"   ✅ {nome} -> {boleto['motivo']}")
                yield emit('file_done', {'filename': nome, 'status': 'success'})
            else:
                yield emit('log', f"   ❌ {nome} (R${boleto['valor']}) -> Sem par")
                yield emit('file_done', {'filename': nome, 'status': 'warning'})
                
            lista_final.append(boleto)

        except Exception as e:
            yield emit('log', f"⚠️ Erro {nome}: {e}")

    # --- 3. ZIP ---
    yield emit('log', '💾 Criando Arquivo...')
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
    nome_zip = f"Conciliacao_Gemini2_{uuid.uuid4().hex[:8]}.zip"
    with open(os.path.join(pasta, nome_zip), 'wb') as f: f.write(output_zip.getvalue())
        
    yield emit('finish', {'url': f"{settings.MEDIA_URL}downloads/{nome_zip}", 'total': len(lista_final)})