"""
PROJETO: Reconciliação de Boletos com Comprovantes
Versão 3.0 - Com OCR (pytesseract) + Regex
- Mais confiável para documentos financeiros
- Mais rápido e barato
- Gemini apenas como fallback opcional
"""

import io
import os
import re
import zipfile
import uuid
import json
import hashlib
import logging
from pypdf import PdfReader, PdfWriter
from pdf2image import convert_from_path, convert_from_bytes
from PIL import Image
import pytesseract
from django.conf import settings
from django.core.cache import cache

# ============================================================
# CONFIGURAÇÃO DE LOGGING
# ============================================================

logger = logging.getLogger(__name__)

# ============================================================
# CONSTANTES
# ============================================================

BATCH_SIZE = 10
CACHE_TTL = 3600 * 24  # 24 horas
TOLERANCIA_VALOR_PERCENT = 0.02  # 2%

# Configuração do Tesseract (ajustar se necessário)
# pytesseract.pytesseract.tesseract_cmd = r'/usr/bin/tesseract'  # Ubuntu/Linux

# ============================================================
# FUNÇÃO: CACHE DE HASH MD5
# ============================================================

def hash_pdf(pdf_bytes):
    """Gera hash MD5 de um PDF para uso em cache."""
    if isinstance(pdf_bytes, io.BytesIO):
        pdf_bytes.seek(0)
        content = pdf_bytes.read()
        pdf_bytes.seek(0)
    else:
        content = pdf_bytes
    
    return hashlib.md5(content).hexdigest()

# ============================================================
# EXTRAÇÃO COM OCR (PRINCIPAL)
# ============================================================

def extrair_com_ocr(pdf_bytes_ou_caminho, use_first_page_only=True, use_cache=True):
    """
    Extrai código de barras e valor usando OCR + Regex.
    Muito mais confiável que Vision AI para documentos financeiros.
    """
    
    try:
        # Cache
        if use_cache:
            if isinstance(pdf_bytes_ou_caminho, bytes):
                pdf_hash = hashlib.md5(pdf_bytes_ou_caminho).hexdigest()
            elif isinstance(pdf_bytes_ou_caminho, io.BytesIO):
                pdf_bytes_ou_caminho.seek(0)
                pdf_hash = hashlib.md5(pdf_bytes_ou_caminho.read()).hexdigest()
                pdf_bytes_ou_caminho.seek(0)
            else:
                with open(pdf_bytes_ou_caminho, 'rb') as f:
                    pdf_hash = hashlib.md5(f.read()).hexdigest()
            
            cache_key = f'ocr_extract_{pdf_hash}'
            cached_result = cache.get(cache_key)
            
            if cached_result:
                logger.debug(f"✓ Cache hit para {pdf_hash[:8]}")
                return cached_result
        
        # Converter PDF para imagem com alta qualidade
        convert_kwargs = {
            'dpi': 300,  # Alta qualidade para OCR
            'fmt': 'png'
        }
        
        if use_first_page_only:
            convert_kwargs['first_page'] = True
        
        if isinstance(pdf_bytes_ou_caminho, bytes):
            images = convert_from_bytes(pdf_bytes_ou_caminho, **convert_kwargs)
        elif isinstance(pdf_bytes_ou_caminho, io.BytesIO):
            pdf_bytes_ou_caminho.seek(0)
            images = convert_from_bytes(pdf_bytes_ou_caminho.read(), **convert_kwargs)
            pdf_bytes_ou_caminho.seek(0)
        else:
            images = convert_from_path(pdf_bytes_ou_caminho, **convert_kwargs)
        
        if not images:
            logger.warning("Nenhuma imagem extraída do PDF")
            return {'codigo': None, 'valor': 0.0, 'valor_formatado': '0,00', 'empresa': 'N/A'}
        
        image = images[0]
        
        # Pré-processar imagem para melhor OCR
        image = preprocessar_imagem(image)
        
        # Extrair texto com OCR
        texto = pytesseract.image_to_string(image, lang='por', config='--psm 6')
        
        logger.debug(f"OCR extraiu {len(texto)} caracteres")
        
        # Extrair informações do texto usando regex
        codigo = extrair_codigo_barras(texto)
        valor = extrair_valor(texto)
        empresa = extrair_empresa(texto)
        
        valor_formatado = converter_para_virgula(f"{valor:.2f}")
        
        resultado = {
            'codigo': codigo,
            'valor': valor,
            'valor_formatado': valor_formatado,
            'empresa': empresa,
            'texto_ocr': texto[:200]  # Primeiros 200 chars para debug
        }
        
        # Salvar no cache
        if use_cache:
            cache.set(cache_key, resultado, CACHE_TTL)
        
        logger.debug(f"OCR resultado: código={codigo[:20] if codigo else 'None'}, valor={valor}")
        return resultado
    
    except Exception as e:
        logger.error(f"⚠️ OCR falhou: {str(e)}", exc_info=True)
        return {'codigo': None, 'valor': 0.0, 'valor_formatado': '0,00', 'empresa': 'N/A'}

# ============================================================
# PRÉ-PROCESSAMENTO DE IMAGEM (MELHORA OCR)
# ============================================================

def preprocessar_imagem(image):
    """
    Aplica filtros para melhorar qualidade do OCR.
    """
    from PIL import ImageEnhance, ImageFilter
    
    # Converter para escala de cinza
    image = image.convert('L')
    
    # Aumentar contraste
    enhancer = ImageEnhance.Contrast(image)
    image = enhancer.enhance(2.0)
    
    # Aumentar nitidez
    enhancer = ImageEnhance.Sharpness(image)
    image = enhancer.enhance(2.0)
    
    # Aplicar threshold (binarização)
    # image = image.point(lambda x: 0 if x < 128 else 255, '1')
    
    return image

# ============================================================
# FUNÇÕES DE EXTRAÇÃO COM REGEX
# ============================================================

def extrair_codigo_barras(texto):
    """
    Extrai código de barras do texto usando regex.
    
    Formatos comuns:
    - Linha digitável: 5 blocos separados por espaços/pontos
    - Código de barras: 44-48 dígitos seguidos
    """
    # Remover espaços e caracteres especiais
    texto_limpo = re.sub(r'[^0-9\s]', '', texto)
    
    # Padrão 1: Sequência de 44-50 dígitos
    pattern1 = r'\b(\d{44,50})\b'
    matches = re.findall(pattern1, texto_limpo)
    if matches:
        return matches[0]
    
    # Padrão 2: Linha digitável (5 blocos)
    # Ex: 34191.79001 01043.510047 91020.150008 2 91070026000
    pattern2 = r'(\d{5}[\.\s]?\d{5}[\.\s]?\d{5}[\.\s]?\d{6}[\.\s]?\d{5}[\.\s]?\d{6}[\.\s]?\d[\.\s]?\d{14})'
    matches = re.findall(pattern2, texto)
    if matches:
        # Remover pontos e espaços
        codigo = re.sub(r'[^0-9]', '', matches[0])
        return codigo
    
    # Padrão 3: Qualquer sequência longa de dígitos
    pattern3 = r'\b(\d{40,})\b'
    matches = re.findall(pattern3, texto_limpo)
    if matches:
        return matches[0]
    
    return None

def extrair_valor(texto):
    """
    Extrai valor monetário do texto.
    
    Procura por padrões como:
    - R$ 1.234,56
    - (=) R$ 1.234,56
    - Valor: 1.234,56
    """
    # Padrões de valor
    patterns = [
        r'R\$\s*(\d{1,3}(?:\.\d{3})*,\d{2})',  # R$ 1.234,56
        r'R\$\s*(\d+,\d{2})',                   # R$ 123,45
        r'(?:Valor|VALOR|Total|TOTAL)[\s:]+R?\$?\s*(\d{1,3}(?:\.\d{3})*,\d{2})',
        r'\(\=\)\s*R\$\s*(\d{1,3}(?:\.\d{3})*,\d{2})',  # (=) R$ 1.234,56
        r'(?:Pagamento|PAGAMENTO)[\s:]+R?\$?\s*(\d{1,3}(?:\.\d{3})*,\d{2})',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, texto, re.IGNORECASE)
        if matches:
            # Pegar o maior valor encontrado (geralmente é o valor total)
            valores = []
            for match in matches:
                try:
                    valor = normalizar_valor(match)
                    if valor > 0:
                        valores.append(valor)
                except:
                    continue
            
            if valores:
                return max(valores)  # Retorna o maior
    
    return 0.0

def extrair_empresa(texto):
    """
    Tenta extrair nome da empresa/cedente.
    Procura por padrões comuns em boletos.
    """
    patterns = [
        r'(?:Beneficiário|BENEFICIÁRIO|Cedente|CEDENTE)[\s:]+([A-ZÀ-Ú][A-ZÀ-Ú\s\.]+)',
        r'(?:Empresa|EMPRESA)[\s:]+([A-ZÀ-Ú][A-ZÀ-Ú\s\.]+)',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, texto)
        if matches:
            empresa = matches[0].strip()
            # Limpar e retornar apenas primeiras palavras
            palavras = empresa.split()[:5]  # Max 5 palavras
            return ' '.join(palavras)
    
    # Fallback: procurar por CNPJ e pegar texto antes
    cnpj_pattern = r'([A-ZÀ-Ú][A-ZÀ-Ú\s\.]+)\s*\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}'
    matches = re.findall(cnpj_pattern, texto)
    if matches:
        return matches[0].strip()
    
    return 'N/A'

# ============================================================
# FUNÇÃO: EXTRAIR VALOR DO NOME DO ARQUIVO
# ============================================================

def extrair_valor_do_nome(nome_arquivo):
    """
    Extrai valor do nome do arquivo como fallback.
    """
    nome = nome_arquivo.replace('.pdf', '').replace('.PDF', '')
    
    patterns = [
        r'R\$\s*([\d.]+,\d{2})',
        r'R\$\s*([\d]+,\d{2})',
        r'R\$\s*([\d]+\.\d{2})',
        r'\b([\d.]+,\d{2})\b',
        r'\b([\d]+\.\d{2})\b',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, nome)
        if matches:
            valor_str = matches[0]
            try:
                if ',' in valor_str and '.' in valor_str:
                    valor = float(valor_str.replace('.', '').replace(',', '.'))
                elif ',' in valor_str:
                    valor = float(valor_str.replace(',', '.'))
                else:
                    valor = float(valor_str)
                
                if valor > 0:
                    logger.debug(f"Valor do nome '{nome_arquivo}': R$ {valor:.2f}")
                    return valor
            except:
                continue
    
    return 0.0

# ============================================================
# UTILITÁRIOS: CONVERSÃO DE NÚMEROS
# ============================================================

def converter_para_virgula(valor_ou_string):
    """Converte para formato brasileiro."""
    if not valor_ou_string:
        return "0,00"
    
    valor_str = str(valor_ou_string).strip()
    
    if ',' in valor_str and '.' not in valor_str:
        return valor_str
    
    if '.' in valor_str and ',' not in valor_str:
        partes = valor_str.split('.')
        
        if len(partes[-1]) == 2:
            numero_sem_pontos = valor_str.replace('.', '')
            return numero_sem_pontos[:-2] + ',' + numero_sem_pontos[-2:]
        else:
            numero_sem_pontos = valor_str.replace('.', '')
            if len(numero_sem_pontos) > 2:
                return numero_sem_pontos[:-2] + ',' + numero_sem_pontos[-2:]
    
    if '.' in valor_str and ',' in valor_str:
        return valor_str
    
    if valor_str.isdigit():
        if len(valor_str) > 2:
            return valor_str[:-2] + ',' + valor_str[-2:]
        else:
            return '0,' + valor_str.zfill(2)
    
    return valor_str

def normalizar_valor(valor):
    """Normaliza qualquer valor para float."""
    if isinstance(valor, (int, float)):
        return float(valor)
    
    if isinstance(valor, str):
        valor = valor.strip()
        valor = valor.replace('R$', '').replace(' ', '').strip()
        
        if ',' in valor:
            valor = valor.replace('.', '').replace(',', '.')
        
        try:
            return float(valor)
        except:
            return 0.0
    
    return 0.0

# ============================================================
# TABELA DE COMPROVANTES
# ============================================================

class TabelaComprovantes:
    def __init__(self):
        self.comprovantes = []
        self.usados = set()
        logger.debug("Tabela de comprovantes inicializada")
    
    def adicionar(self, id_comp, codigo, valor, valor_formatado, empresa, pdf_bytes):
        item = {
            'id': id_comp,
            'codigo': codigo,
            'valor': valor,
            'valor_formatado': valor_formatado,
            'empresa': empresa,
            'pdf_bytes': pdf_bytes,
        }
        self.comprovantes.append(item)
        logger.debug(f"Comprovante {id_comp} adicionado: R$ {valor_formatado}")
        return item
    
    def buscar_por_codigo(self, codigo):
        if not codigo:
            return None
        
        codigo_limpo = re.sub(r'[^0-9]', '', str(codigo))
        
        for comp in self.comprovantes:
            if comp['id'] in self.usados:
                continue
            
            if comp['codigo']:
                codigo_comp_limpo = re.sub(r'[^0-9]', '', str(comp['codigo']))
                
                if codigo_limpo == codigo_comp_limpo:
                    logger.info(f"✓ Match EXATO por código")
                    return comp
                elif len(codigo_limpo) > 20 and (codigo_limpo in codigo_comp_limpo or codigo_comp_limpo in codigo_limpo):
                    logger.info(f"✓ Match PARCIAL por código")
                    return comp
        
        return None
    
    def buscar_por_valor(self, valor, tolerancia_percent=TOLERANCIA_VALOR_PERCENT):
        if valor == 0:
            return None
        
        diferenca_maxima = valor * tolerancia_percent
        melhor_match = None
        menor_diferenca = float('inf')
        
        for comp in self.comprovantes:
            if comp['id'] in self.usados:
                continue
            
            diferenca = abs(comp['valor'] - valor)
            
            if diferenca <= diferenca_maxima and diferenca < menor_diferenca:
                melhor_match = comp
                menor_diferenca = diferenca
        
        if melhor_match:
            logger.info(f"✓ Match por valor: R$ {valor:.2f} ≈ R$ {melhor_match['valor']:.2f}")
        
        return melhor_match
    
    def buscar_por_empresa_e_valor(self, empresa, valor, tolerancia_percent=TOLERANCIA_VALOR_PERCENT):
        if not empresa or empresa == 'N/A' or valor == 0:
            return None
        
        empresa_limpa = empresa.lower().strip()
        diferenca_maxima = valor * tolerancia_percent
        
        for comp in self.comprovantes:
            if comp['id'] in self.usados:
                continue
            
            comp_empresa = str(comp['empresa']).lower().strip()
            
            if empresa_limpa in comp_empresa or comp_empresa in empresa_limpa:
                if abs(comp['valor'] - valor) <= diferenca_maxima:
                    logger.info(f"✓ Match por EMPRESA + VALOR")
                    return comp
        
        return None
    
    def marcar_usado(self, id_comp):
        self.usados.add(id_comp)
    
    def get_stats(self):
        return {
            'total': len(self.comprovantes),
            'usados': len(self.usados),
            'disponiveis': len(self.comprovantes) - len(self.usados)
        }

# ============================================================
# PROCESSAMENTO PRINCIPAL
# ============================================================

def processar_reconciliacao(caminho_comprovantes, lista_caminhos_boletos, user):
    """
    Processamento com OCR + Regex (versão melhorada).
    """
    
    def emit(tipo, dados):
        return json.dumps({'type': tipo, 'data': dados}) + "\n"
    
    yield emit('log', '🚀 Iniciando processamento com OCR...')
    yield emit('log', '📋 ETAPA 1: Lendo arquivo de comprovantes')
    
    tabela = TabelaComprovantes()
    
    try:
        reader_comp = PdfReader(caminho_comprovantes)
        total_paginas = len(reader_comp.pages)
        
        yield emit('log', f'📄 Total de páginas: {total_paginas}')
        yield emit('log', f'🔍 Usando OCR (Tesseract) para extrair dados...')
        
        for idx, page in enumerate(reader_comp.pages):
            writer = PdfWriter()
            writer.add_page(page)
            bio = io.BytesIO()
            writer.write(bio)
            bio.seek(0)
            
            yield emit('log', f'  [OCR] Analisando página {idx+1}/{total_paginas}...')
            
            try:
                dados_ocr = extrair_com_ocr(bio, use_cache=True)
                
                tabela.adicionar(
                    id_comp=idx,
                    codigo=dados_ocr['codigo'],
                    valor=dados_ocr['valor'],
                    valor_formatado=dados_ocr['valor_formatado'],
                    empresa=dados_ocr['empresa'],
                    pdf_bytes=bio
                )
                
                cod_display = (dados_ocr['codigo'][:25] + "...") if dados_ocr['codigo'] else "SEM_CODIGO"
                yield emit('log', f'  ✓ Pág {idx+1}: R$ {dados_ocr["valor_formatado"]} | {cod_display} | {dados_ocr["empresa"]}')
                yield emit('comp_status', {'index': idx, 'msg': f'R$ {dados_ocr["valor_formatado"]}'})
                
            except Exception as e:
                logger.error(f"Erro página {idx+1}: {e}")
                yield emit('log', f'  ⚠️ Erro na página {idx+1}')
                tabela.adicionar(idx, None, 0.0, '0,00', 'N/A', bio)
    
    except Exception as e:
        logger.error(f'Erro ao ler comprovantes: {e}', exc_info=True)
        yield emit('log', f'❌ ERRO: {str(e)}')
        return
    
    # ETAPA 2: PROCESSAR BOLETOS
    yield emit('log', '')
    yield emit('log', '📑 ETAPA 2: Processando boletos com OCR')
    yield emit('log', f'Total de boletos: {len(lista_caminhos_boletos)}')
    
    resultados = []
    
    for i, caminho_boleto in enumerate(lista_caminhos_boletos):
        nome_boleto = os.path.basename(caminho_boleto)
        
        yield emit('file_start', {'filename': nome_boleto})
        yield emit('log', f'')
        yield emit('log', f'📄 Boleto {i+1}/{len(lista_caminhos_boletos)}: {nome_boleto}')
        
        try:
            with open(caminho_boleto, 'rb') as f:
                pdf_bytes = f.read()
            
            yield emit('log', f'   [OCR] Analisando boleto...')
            
            dados_ocr = extrair_com_ocr(pdf_bytes, use_cache=True)
            
            codigo_boleto = dados_ocr['codigo']
            valor_boleto = dados_ocr['valor']
            empresa_boleto = dados_ocr['empresa']
            
            # Fallback: nome do arquivo
            if valor_boleto == 0.0:
                valor_boleto = extrair_valor_do_nome(nome_boleto)
                if valor_boleto > 0:
                    yield emit('log', f'   [Fallback] Valor do nome: R$ {converter_para_virgula(f"{valor_boleto:.2f}")}')
            
            valor_boleto_formatado = converter_para_virgula(f"{valor_boleto:.2f}")
            bio_boleto = io.BytesIO(pdf_bytes)
            
            yield emit('log', f'   → Código: {(codigo_boleto[:30] + "...") if codigo_boleto else "N/A"}')
            yield emit('log', f'   → Valor: R$ {valor_boleto_formatado}')
            yield emit('log', f'   → Empresa: {empresa_boleto}')
            
            # MATCH
            comprovante_encontrado = None
            metodo_match = None
            
            if codigo_boleto:
                comp = tabela.buscar_por_codigo(codigo_boleto)
                if comp:
                    comprovante_encontrado = comp
                    metodo_match = "CÓDIGO"
                    yield emit('log', f'   ✅ MATCH por CÓDIGO (pág {comp["id"]+1})')
            
            if not comprovante_encontrado and empresa_boleto != 'N/A' and valor_boleto > 0:
                comp = tabela.buscar_por_empresa_e_valor(empresa_boleto, valor_boleto)
                if comp:
                    comprovante_encontrado = comp
                    metodo_match = "EMPRESA+VALOR"
                    yield emit('log', f'   ✅ MATCH por EMPRESA+VALOR (pág {comp["id"]+1})')
            
            if not comprovante_encontrado and valor_boleto > 0:
                comp = tabela.buscar_por_valor(valor_boleto)
                if comp:
                    comprovante_encontrado = comp
                    metodo_match = "VALOR"
                    yield emit('log', f'   ⚠️ MATCH por VALOR (pág {comp["id"]+1})')
            
            status = 'warning'
            if comprovante_encontrado:
                tabela.marcar_usado(comprovante_encontrado['id'])
                status = 'success' if metodo_match in ['CÓDIGO', 'EMPRESA+VALOR'] else 'warning'
                
                resultados.append({
                    'boleto_nome': nome_boleto,
                    'boleto_codigo': codigo_boleto,
                    'boleto_valor': valor_boleto,
                    'boleto_valor_formatado': valor_boleto_formatado,
                    'boleto_empresa': empresa_boleto,
                    'boleto_pdf': bio_boleto,
                    'comprovante': comprovante_encontrado,
                    'metodo': metodo_match
                })
            else:
                yield emit('log', f'   ❌ SEM MATCH')
                resultados.append({
                    'boleto_nome': nome_boleto,
                    'boleto_codigo': codigo_boleto,
                    'boleto_valor': valor_boleto,
                    'boleto_valor_formatado': valor_boleto_formatado,
                    'boleto_empresa': empresa_boleto,
                    'boleto_pdf': bio_boleto,
                    'comprovante': None,
                    'metodo': None
                })
            
            yield emit('file_done', {'filename': nome_boleto, 'status': status})
            
            if (i + 1) % BATCH_SIZE == 0:
                import gc
                gc.collect()
        
        except Exception as e:
            logger.error(f'Erro boleto {nome_boleto}: {e}', exc_info=True)
            yield emit('log', f'   ❌ ERRO: {str(e)[:100]}')
            yield emit('file_done', {'filename': nome_boleto, 'status': 'error'})
    
    # ETAPA 3: ZIP
    yield emit('log', '')
    yield emit('log', '💾 ETAPA 3: Gerando ZIP')
    
    output_zip = io.BytesIO()
    
    try:
        with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for resultado in resultados:
                try:
                    writer_final = PdfWriter()
                    
                    resultado['boleto_pdf'].seek(0)
                    reader_boleto = PdfReader(resultado['boleto_pdf'])
                    for page in reader_boleto.pages:
                        writer_final.add_page(page)
                    
                    if resultado['comprovante']:
                        resultado['comprovante']['pdf_bytes'].seek(0)
                        reader_comp = PdfReader(resultado['comprovante']['pdf_bytes'])
                        for page in reader_comp.pages:
                            writer_final.add_page(page)
                    
                    bio_final = io.BytesIO()
                    writer_final.write(bio_final)
                    zip_file.writestr(resultado['boleto_nome'], bio_final.getvalue())
                
                except Exception as e:
                    yield emit('log', f'   ❌ Erro: {resultado["boleto_nome"]}')
    
    except Exception as e:
        yield emit('log', f'❌ ERRO ZIP: {str(e)}')
        return
    
    pasta_downloads = os.path.join(settings.MEDIA_ROOT, 'downloads')
    os.makedirs(pasta_downloads, exist_ok=True)
    
    nome_zip = f"Reconciliacao_{uuid.uuid4().hex[:8]}.zip"
    caminho_zip = os.path.join(pasta_downloads, nome_zip)
    
    with open(caminho_zip, 'wb') as f:
        f.write(output_zip.getvalue())
    
    url_download = f"{settings.MEDIA_URL}downloads/{nome_zip}"
    
    total_boletos = len(resultados)
    total_matches = len([r for r in resultados if r['comprovante']])
    total_sem_match = total_boletos - total_matches
    
    matches_codigo = len([r for r in resultados if r.get('metodo') == 'CÓDIGO'])
    matches_empresa_valor = len([r for r in resultados if r.get('metodo') == 'EMPRESA+VALOR'])
    matches_valor = len([r for r in resultados if r.get('metodo') == 'VALOR'])
    
    stats_tabela = tabela.get_stats()
    
    yield emit('log', '')
    yield emit('log', '✅ PROCESSO CONCLUÍDO!')
    yield emit('log', f'📊 RESUMO:')
    yield emit('log', f'   Total: {total_boletos}')
    yield emit('log', f'   ✓ Matches: {total_matches}')
    yield emit('log', f'     - Código: {matches_codigo}')
    yield emit('log', f'     - Empresa+Valor: {matches_empresa_valor}')
    yield emit('log', f'     - Valor: {matches_valor}')
    yield emit('log', f'   ❌ Sem match: {total_sem_match}')
    yield emit('log', f'   Comprovantes não usados: {stats_tabela["disponiveis"]}')
    yield emit('log', f'📦 Arquivo: {nome_zip}')
    
    yield emit('finish', {
        'url': url_download,
        'total': total_boletos,
        'matches': total_matches,
        'sem_match': total_sem_match,
        'matches_codigo': matches_codigo,
        'matches_empresa_valor': matches_empresa_valor,
        'matches_valor': matches_valor,
        'comprovantes_disponiveis': stats_tabela['disponiveis']
    })