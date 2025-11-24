import io
import os
import zipfile
import uuid
import json
import re
import logging
import shutil
from difflib import SequenceMatcher # <--- A Mágica da Similaridade
from pypdf import PdfReader, PdfWriter
from pdf2image import convert_from_bytes
import pytesseract
from django.conf import settings

# Logger
logger = logging.getLogger(__name__)

# Configura Tesseract
if shutil.which('tesseract'):
    pytesseract.pytesseract.tesseract_cmd = shutil.which('tesseract')
else:
    pytesseract.pytesseract.tesseract_cmd = r'/usr/bin/tesseract'

# ============================================================
# FERRAMENTAS MATEMÁTICAS
# ============================================================

def calcular_similaridade(a, b):
    """
    Retorna uma nota de 0.0 a 1.0 de semelhança entre duas strings numéricas.
    Ex: 
    '123456' e '123456' -> 1.0 (100%)
    '123456' e '123499' -> 0.66 (66%)
    """
    if not a or not b: return 0.0
    return SequenceMatcher(None, a, b).ratio()

def limpar_numeros(texto):
    """Deixa só numeros."""
    if not texto: return ""
    return re.sub(r'\D', '', str(texto))

def normalizar_valor(v_str):
    try:
        v = str(v_str).replace('R$', '').strip()
        if ',' in v and '.' in v: v = v.replace('.', '').replace(',', '.')
        elif ',' in v: v = v.replace(',', '.')
        return float(v)
    except: return 0.0

def formatar_br(valor):
    return f"{valor:,.2f}".replace('.', 'X').replace(',', '.').replace('X', ',')

# ============================================================
# EXTRAÇÃO DE DADOS (TEXTO -> OCR -> NOME)
# ============================================================

def extrair_valor_nome(nome):
    match = re.search(r'R\$\s?(\d+)[_.,-](\d{2})', nome)
    if match:
        try: return float(f"{match.group(1)}.{match.group(2)}")
        except: pass
    return 0.0

def regex_busca(texto):
    dados = {'codigo': '', 'valor': 0.0}
    # Código (longo)
    clean = re.sub(r'[\s\.\-\_]', '', texto)
    match = re.search(r'\d{36,60}', clean)
    if match: dados['codigo'] = match.group(0)
    # Valor
    vals = re.findall(r'(?:R\$\s?)?(\d{1,3}(?:\.?\d{3})*,\d{2})', texto)
    floats = [normalizar_valor(v) for v in vals]
    if floats: dados['valor'] = max(floats)
    return dados

def extrair_inteligente(pdf_bytes, nome_arquivo=""):
    res = {'codigo': '', 'valor': 0.0, 'origem': ''}
    try:
        # 1. TEXTO
        reader = PdfReader(io.BytesIO(pdf_bytes))
        txt = ""
        for p in reader.pages: txt += p.extract_text() + "\n"
        if len(txt) > 20:
            d = regex_busca(txt)
            if d['valor'] > 0 or d['codigo']:
                res.update(d)
                res['origem'] = 'TEXTO'
        
        # 2. OCR (Se falhou)
        if res['valor'] == 0 and not res['codigo']:
            imgs = convert_from_bytes(pdf_bytes, dpi=200, fmt='jpeg', first_page=True)
            if imgs:
                txt_ocr = pytesseract.image_to_string(imgs[0], lang='por')
                d = regex_busca(txt_ocr)
                if d['valor'] > 0 or d['codigo']:
                    res.update(d)
                    res['origem'] = 'OCR'
    except: pass

    # 3. NOME (Valor apenas)
    if res['valor'] == 0 and nome_arquivo:
        v = extrair_valor_nome(nome_arquivo)
        if v > 0:
            res['valor'] = v
            res['origem'] += '+NOME'
            
    return res

# ============================================================
# FLUXO PRINCIPAL
# ============================================================

def processar_reconciliacao(caminho_comprovantes, lista_caminhos_boletos, user):
    
    def emit(tipo, dados):
        return json.dumps({'type': tipo, 'data': dados}) + "\n"
    
    yield emit('log', '🚀 Iniciando (Modo: Similaridade de Código)...')

    # --- 1. LER COMPROVANTES (INVENTÁRIO) ---
    yield emit('log', '📂 Indexando Comprovantes...')
    pool_comprovantes = []
    
    try:
        reader = PdfReader(caminho_comprovantes)
        for i, page in enumerate(reader.pages):
            writer = PdfWriter()
            writer.add_page(page)
            bio = io.BytesIO()
            writer.write(bio)
            b_pag = bio.getvalue()
            
            # Extrai
            d = extrair_inteligente(b_pag)
            cod = limpar_numeros(d['codigo'])
            
            item = {
                'id': i,
                'codigo': cod,
                'valor': d['valor'],
                'pdf_bytes': b_pag,
                'usado': False
            }
            pool_comprovantes.append(item)
            
            show_cod = f"...{cod[-8:]}" if cod else "SEM_COD"
            yield emit('comp_status', {'index': i, 'msg': f"R${item['valor']} ({show_cod})"})

    except Exception as e:
        yield emit('log', f"❌ Erro leitura: {e}")
        return

    # --- 2. LER BOLETOS E ENCONTRAR O MELHOR PAR ---
    yield emit('log', '⚡ Analisando Boletos e buscando Similaridade...')
    lista_final = []

    for path in lista_caminhos_boletos:
        nome = os.path.basename(path)
        yield emit('file_start', {'filename': nome})
        
        try:
            with open(path, 'rb') as f: pdf_bytes = f.read()
            d = extrair_inteligente(pdf_bytes, nome)
            
            boleto = {
                'nome': nome,
                'codigo': limpar_numeros(d['codigo']),
                'valor': d['valor'],
                'pdf_bytes': pdf_bytes,
                'match': None,
                'motivo': ''
            }
            
            # === A MÁGICA DO MATCH ===
            melhor_candidato = None
            maior_nota = 0.0
            
            if boleto['valor'] > 0:
                # Filtra APENAS comprovantes com o MESMO VALOR (Margem de 5 centavos)
                candidatos = [c for c in pool_comprovantes if not c['usado'] and abs(c['valor'] - boleto['valor']) < 0.05]
                
                if candidatos:
                    # Dentre os candidatos de mesmo valor, quem tem o código mais parecido?
                    for cand in candidatos:
                        # Se não tem código, a nota é 0. Se tem, calcula similaridade.
                        nota = calcular_similaridade(boleto['codigo'], cand['codigo'])
                        
                        # Debug no log para você ver a mágica acontecendo
                        # yield emit('log', f"   ⚖️ Comparando com Pag {cand['id']+1}: {int(nota*100)}% de chance")
                        
                        if nota > maior_nota:
                            maior_nota = nota
                            melhor_candidato = cand
                    
                    # DECISÃO
                    # Se tiver código similar (> 60%) OU se for o único candidato de valor (nota 0 mas unico)
                    eh_match = False
                    
                    if boleto['codigo'] and maior_nota > 0.6:
                        eh_match = True
                        boleto['motivo'] = f"SIMILARIDADE ({int(maior_nota*100)}%)"
                    elif not boleto['codigo'] and len(candidatos) == 1:
                        # Se boleto nao tem código legivel, mas só tem 1 comprovante desse valor
                        eh_match = True
                        boleto['motivo'] = "VALOR (Único Disponível)"
                    elif boleto['codigo'] and maior_nota < 0.6 and len(candidatos) == 1:
                        # Codigo muito diferente, mas é o unico valor disponivel
                        eh_match = True
                        boleto['motivo'] = "VALOR (Único, Baixa Simil.)"
                    # Se tiver varios candidatos de mesmo valor e nenhum código parecido, NÃO CASA (evita erro)
                    
                    if eh_match and melhor_candidato:
                        boleto['match'] = melhor_candidato
                        melhor_candidato['usado'] = True
                        yield emit('log', f"   ✅ {nome} -> {boleto['motivo']}")
                        yield emit('file_done', {'filename': nome, 'status': 'success'})
                    else:
                         yield emit('log', f"   ⚠️ {nome} (R${boleto['valor']}) -> Vários valores iguais, nenhum código similar.")
                         yield emit('file_done', {'filename': nome, 'status': 'warning'})

                else:
                    yield emit('log', f"   ❌ {nome} (R${boleto['valor']}) -> Sem comprovante deste valor.")
                    yield emit('file_done', {'filename': nome, 'status': 'warning'})
            else:
                yield emit('log', f"   ❌ {nome} -> Valor Zero/Ilegível")
                yield emit('file_done', {'filename': nome, 'status': 'warning'})
                
            lista_final.append(boleto)

        except Exception as e:
            yield emit('log', f"⚠️ Erro: {e}")

    # --- 3. GERAR ZIP ---
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

    # Finaliza
    pasta = os.path.join(settings.MEDIA_ROOT, 'downloads')
    os.makedirs(pasta, exist_ok=True)
    nome_zip = f"Conciliacao_Smart_{uuid.uuid4().hex[:8]}.zip"
    with open(os.path.join(pasta, nome_zip), 'wb') as f: f.write(output_zip.getvalue())
        
    yield emit('finish', {'url': f"{settings.MEDIA_URL}downloads/{nome_zip}", 'total': len(lista_final)})