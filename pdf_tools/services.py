"""
PROJETO: Reconciliação de Boletos com Comprovantes
Usando Google Gemini Vision + Conversão de formato de números (. → ,)
"""

import io
import os
import re
import zipfile
import uuid
import json
import base64
from pypdf import PdfReader, PdfWriter
from pdf2image import convert_from_path, convert_from_bytes
from PIL import Image
import google.generativeai as genai
from django.conf import settings

# ============================================================
# CONFIGURAR GEMINI COM CHAVE
# ============================================================

GEMINI_API_KEY = 'AIzaSyAeFFYrpTCRDP9NZPRRwoa4vC8KWE7UNVQ'

genai.configure(api_key=GEMINI_API_KEY)
print(f"✅ Gemini configurado com sucesso!")

# ============================================================
# UTILITÁRIOS: CONVERSÃO DE NÚMEROS
# ============================================================

def converter_para_virgula(valor_ou_string):
    """
    Converte número de formato com ponto para vírgula (formato brasileiro).
    
    Exemplos:
    - 402.00 → 402,00
    - 402,00 → 402,00
    - 1234.56 → 1.234,56
    - 1,234.56 → 1.234,56
    """
    if not valor_ou_string:
        return "0,00"
    
    valor_str = str(valor_ou_string).strip()
    
    # Se já tem vírgula (tipo 402,00), retorna como está
    if ',' in valor_str and '.' not in valor_str:
        return valor_str
    
    # Se tem ponto como decimal (tipo 402.00 ou 1234.56)
    if '.' in valor_str and ',' not in valor_str:
        partes = valor_str.split('.')
        
        if len(partes[-1]) == 2:
            numero_sem_pontos = valor_str.replace('.', '')
            return numero_sem_pontos[:-2] + ',' + numero_sem_pontos[-2:]
        else:
            numero_sem_pontos = valor_str.replace('.', '')
            if len(numero_sem_pontos) > 2:
                return numero_sem_pontos[:-2] + ',' + numero_sem_pontos[-2:]
    
    # Formato misto (1.234,56) - já está certo
    if '.' in valor_str and ',' in valor_str:
        return valor_str
    
    # Se é só número sem separadores
    if valor_str.isdigit():
        if len(valor_str) > 2:
            return valor_str[:-2] + ',' + valor_str[-2:]
        else:
            return '0,' + valor_str.zfill(2)
    
    return valor_str


def normalizar_valor(valor):
    """
    Normaliza qualquer tipo de valor (string, float, int) para float.
    Entende múltiplos formatos.
    """
    if isinstance(valor, (int, float)):
        return float(valor)
    
    if isinstance(valor, str):
        valor = valor.strip()
        
        # Remove R$ se tiver
        valor = valor.replace('R$', '').strip()
        
        # Converte vírgula em ponto se necessário
        if ',' in valor:
            valor = valor.replace('.', '').replace(',', '.')
        
        try:
            return float(valor)
        except:
            return 0.0
    
    return 0.0

# ============================================================
# 1. EXTRAÇÃO COM GEMINI VISION
# ============================================================

def extrair_com_gemini(pdf_bytes_ou_caminho, use_first_page_only=True):
    """
    Usa Google Gemini Vision para extrair código de barras e valor de um PDF.
    Retorna: {'codigo': '...', 'valor': 0.0, 'valor_formatado': 'XXX,XX', 'empresa': '...'}
    """
    
    try:
        # Converter PDF para imagens
        if isinstance(pdf_bytes_ou_caminho, bytes):
            images = convert_from_bytes(pdf_bytes_ou_caminho, first_page=use_first_page_only)
        else:
            images = convert_from_path(pdf_bytes_ou_caminho, first_page=use_first_page_only)
        
        if not images:
            return {'codigo': None, 'valor': 0.0, 'valor_formatado': '0,00', 'empresa': 'N/A'}
        
        # Pega primeira página
        image = images[0]
        
        # Converter imagem para base64
        img_buffer = io.BytesIO()
        image.save(img_buffer, format='PNG')
        img_base64 = base64.standard_b64encode(img_buffer.getvalue()).decode('utf-8')
        
        # Chamar Gemini Vision
        model = genai.GenerativeModel('gemini-2.0-flash')
        
        response = model.generate_content(
            [
                {
                    "mime_type": "image/png",
                    "data": img_base64,
                },
                f"""Analise esta imagem de um documento (boleto, comprovante ou similar) e extraia:

1. CÓDIGO DE BARRAS: A sequência numérica longa (geralmente 47-50 dígitos).
2. VALOR: O valor em reais (formato R$ XXX,XX ou similar)
3. EMPRESA/CEDENTE: Nome da empresa ou pessoa

Responda em JSON com EXATAMENTE este formato (sem markdown, só JSON puro):
{{
  "codigo": "número ou null se não encontrar",
  "valor": "valor como número com ponto (ex: 402.00 ou 1234.56) ou null",
  "empresa": "nome da empresa ou 'N/A'"
}}

IMPORTANTE: 
- Se houver um código de barras na imagem, extraia TODOS os números
- O valor deve ser um número com ponto como decimal (ex: 402.00 não '402,00')
- Se não encontrar, coloque null ou 0.00"""
            ]
        )
        
        # Parse resposta JSON
        response_text = response.text
        
        # Limpar markdown se tiver
        if "```json" in response_text:
            response_text = response_text.split("```json")[1].split("```")[0]
        elif "```" in response_text:
            response_text = response_text.split("```")[1].split("```")[0]
        
        response_text = response_text.strip()
        dados = json.loads(response_text)
        
        # Converter valor para float
        valor = 0.0
        if dados.get('valor') and dados['valor'] not in ['0.00', 'null', None]:
            valor = normalizar_valor(dados['valor'])
        
        # Converter para formato brasileiro (com vírgula)
        valor_formatado = converter_para_virgula(f"{valor:.2f}")
        
        return {
            'codigo': dados.get('codigo'),
            'valor': valor,
            'valor_formatado': valor_formatado,
            'empresa': dados.get('empresa', 'N/A') or 'N/A'
        }
    
    except Exception as e:
        print(f"❌ Erro ao processar com Gemini: {str(e)}")
        return {'codigo': None, 'valor': 0.0, 'valor_formatado': '0,00', 'empresa': 'N/A'}

# ============================================================
# 2. FALLBACK: Extração com regex
# ============================================================

def extrair_valor_fallback(texto):
    """Fallback para extrair valor se Gemini falhar."""
    if not texto:
        return 0.0
    
    matches = re.findall(r'R\$\s*([\d.]+,\d{2})', texto)
    if not matches:
        matches = re.findall(r'R\$\s*(\d+,\d{2})', texto)
    
    if matches:
        try:
            return float(matches[0].replace('.', '').replace(',', '.'))
        except:
            pass
    
    return 0.0

# ============================================================
# 3. TABELA TEMPORÁRIA
# ============================================================

class TabelaComprovantes:
    def __init__(self):
        self.comprovantes = []
        self.usados = set()
    
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
        return item
    
    def buscar_por_codigo(self, codigo):
        if not codigo:
            return None
        
        for comp in self.comprovantes:
            if comp['id'] in self.usados:
                continue
            
            if comp['codigo']:
                if codigo in comp['codigo'] or comp['codigo'] in codigo:
                    return comp
        
        return None
    
    def buscar_por_valor(self, valor, tolerancia=0.05):
        if valor == 0:
            return None
        
        for comp in self.comprovantes:
            if comp['id'] in self.usados:
                continue
            
            if abs(comp['valor'] - valor) < tolerancia:
                return comp
        
        return None
    
    def marcar_usado(self, id_comp):
        self.usados.add(id_comp)
    
    def listar_nao_usados(self):
        return [c for c in self.comprovantes if c['id'] not in self.usados]

# ============================================================
# 4. PROCESSAMENTO PRINCIPAL
# ============================================================

def processar_reconciliacao(caminho_comprovantes, lista_caminhos_boletos, user):
    """
    Processamento com Google Gemini Vision + conversão de números.
    """
    
    def emit(tipo, dados):
        return json.dumps({'type': tipo, 'data': dados}) + "\n"
    
    # ========================================================
    # ETAPA 1: CARREGAR COMPROVANTES
    # ========================================================
    
    yield emit('log', '🚀 Iniciando processamento com Gemini Vision...')
    yield emit('log', '📋 ETAPA 1: Lendo arquivo de comprovantes')
    
    tabela = TabelaComprovantes()
    
    try:
        reader_comp = PdfReader(caminho_comprovantes)
        total_paginas = len(reader_comp.pages)
        
        yield emit('log', f'📄 Total de páginas: {total_paginas}')
        yield emit('log', f'🤖 Usando Google Gemini para extrair códigos...')
        
        for idx, page in enumerate(reader_comp.pages):
            # Extrair texto simples como fallback
            texto = page.extract_text() or ""
            
            # Salvar página como PDF bytes
            writer = PdfWriter()
            writer.add_page(page)
            bio = io.BytesIO()
            writer.write(bio)
            bio.seek(0)
            
            # Usar Gemini Vision para extrair dados
            yield emit('log', f'  [Gemini] Analisando página {idx+1}...')
            dados_gemini = extrair_com_gemini(bio)
            
            codigo = dados_gemini['codigo']
            valor = dados_gemini['valor']
            valor_formatado = dados_gemini['valor_formatado']
            empresa = dados_gemini['empresa']
            
            # Fallback: se Gemini não achou valor, tenta regex
            if valor == 0.0:
                valor = extrair_valor_fallback(texto)
                valor_formatado = converter_para_virgula(f"{valor:.2f}")
            
            # Adicionar à tabela
            item = tabela.adicionar(
                id_comp=idx,
                codigo=codigo,
                valor=valor,
                valor_formatado=valor_formatado,
                empresa=empresa,
                pdf_bytes=bio
            )
            
            # Log com valor formatado em vírgula
            cod_display = codigo[:25] + "..." if codigo else "SEM_CODIGO"
            yield emit('log', f'  ✓ Pág {idx+1}: R$ {valor_formatado} | {cod_display} | {empresa}')
            yield emit('comp_status', {'index': idx, 'msg': f'R$ {valor_formatado}'})
    
    except Exception as e:
        yield emit('log', f'❌ ERRO ao ler comprovantes: {str(e)}')
        import traceback
        yield emit('log', f'Detalhes: {traceback.format_exc()[:300]}')
        return
    
    # ========================================================
    # ETAPA 2: PROCESSAR BOLETOS
    # ========================================================
    
    yield emit('log', '')
    yield emit('log', '📑 ETAPA 2: Processando boletos com Gemini')
    yield emit('log', f'Total de boletos: {len(lista_caminhos_boletos)}')
    
    resultados = []
    
    for i, caminho_boleto in enumerate(lista_caminhos_boletos):
        nome_boleto = os.path.basename(caminho_boleto)
        
        yield emit('file_start', {'filename': nome_boleto})
        yield emit('log', f'')
        yield emit('log', f'📄 Boleto {i+1}/{len(lista_caminhos_boletos)}: {nome_boleto}')
        
        try:
            # Usar Gemini Vision para extrair dados do boleto
            yield emit('log', f'   [Gemini] Analisando boleto...')
            
            with open(caminho_boleto, 'rb') as f:
                pdf_bytes = f.read()
            
            dados_gemini = extrair_com_gemini(pdf_bytes)
            
            codigo_boleto = dados_gemini['codigo']
            valor_boleto = dados_gemini['valor']
            valor_boleto_formatado = dados_gemini['valor_formatado']
            
            # Salvar boleto como bytes
            bio_boleto = io.BytesIO(pdf_bytes)
            bio_boleto.seek(0)
            
            yield emit('log', f'   → Código: {codigo_boleto[:30] if codigo_boleto else "N/A"}')
            yield emit('log', f'   → Valor: R$ {valor_boleto_formatado}')
            
            # ====================================================
            # TENTAR MATCH
            # ====================================================
            
            comprovante_encontrado = None
            metodo_match = None
            
            # 1️⃣ Tentar por CÓDIGO
            if codigo_boleto:
                comp = tabela.buscar_por_codigo(codigo_boleto)
                if comp:
                    comprovante_encontrado = comp
                    metodo_match = "CÓDIGO"
                    yield emit('log', f'   ✅ MATCH por CÓDIGO (página {comp["id"]+1})')
            
            # 2️⃣ Tentar por VALOR (se não achou por código)
            if not comprovante_encontrado and valor_boleto > 0:
                comp = tabela.buscar_por_valor(valor_boleto)
                if comp:
                    comprovante_encontrado = comp
                    metodo_match = "VALOR"
                    yield emit('log', f'   ✅ MATCH por VALOR (página {comp["id"]+1})')
            
            # Guardar resultado
            status = 'warning'
            if comprovante_encontrado:
                tabela.marcar_usado(comprovante_encontrado['id'])
                status = 'success'
                resultados.append({
                    'boleto_nome': nome_boleto,
                    'boleto_codigo': codigo_boleto,
                    'boleto_valor': valor_boleto,
                    'boleto_valor_formatado': valor_boleto_formatado,
                    'boleto_pdf': bio_boleto,
                    'comprovante': comprovante_encontrado,
                    'metodo': metodo_match
                })
            else:
                yield emit('log', f'   ❌ SEM MATCH ENCONTRADO')
                resultados.append({
                    'boleto_nome': nome_boleto,
                    'boleto_codigo': codigo_boleto,
                    'boleto_valor': valor_boleto,
                    'boleto_valor_formatado': valor_boleto_formatado,
                    'boleto_pdf': bio_boleto,
                    'comprovante': None,
                    'metodo': None
                })
            
            yield emit('file_done', {'filename': nome_boleto, 'status': status})
        
        except Exception as e:
            yield emit('log', f'   ❌ ERRO: {str(e)}')
            yield emit('file_done', {'filename': nome_boleto, 'status': 'error'})
            continue
    
    # ========================================================
    # ETAPA 3: GERAR ZIP
    # ========================================================
    
    yield emit('log', '')
    yield emit('log', '💾 ETAPA 3: Gerando arquivo ZIP')
    
    output_zip = io.BytesIO()
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        
        for resultado in resultados:
            nome_boleto = resultado['boleto_nome']
            
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
                bio_final.seek(0)
                
                zip_file.writestr(nome_boleto, bio_final.getvalue())
            
            except Exception as e:
                yield emit('log', f'   ❌ ERRO ao gerar {nome_boleto}: {str(e)}')
                continue
    
    # ========================================================
    # FINALIZAR
    # ========================================================
    
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
    
    yield emit('log', '')
    yield emit('log', '✅ PROCESSO CONCLUÍDO!')
    yield emit('log', f'📊 RESUMO:')
    yield emit('log', f'   Total de boletos: {total_boletos}')
    yield emit('log', f'   Encontrados: {total_matches}')
    yield emit('log', f'   Sem match: {total_sem_match}')
    yield emit('log', f'📦 Arquivo gerado!')
    
    yield emit('finish', {
        'url': url_download,
        'total': total_boletos,
        'matches': total_matches,
        'sem_match': total_sem_match
    })