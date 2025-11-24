"""
PROJETO: Reconciliação de Boletos com Comprovantes
Versão 2.0 - Melhorada com:
- Variáveis de ambiente para API keys
- Cache de resultados Gemini
- Melhor tratamento de erros
- Processamento em lotes
- Logging robusto
- Tolerância percentual no match
"""

import io
import os
import re
import zipfile
import uuid
import json
import base64
import hashlib
import logging
from pypdf import PdfReader, PdfWriter
from pdf2image import convert_from_path, convert_from_bytes
from PIL import Image
import google.generativeai as genai
from django.conf import settings
from django.core.cache import cache

# ============================================================
# CONFIGURAÇÃO DE LOGGING
# ============================================================

logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURAR GEMINI COM VARIÁVEL DE AMBIENTE
# ============================================================

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', settings.GEMINI_API_KEY if hasattr(settings, 'GEMINI_API_KEY') else None)

if not GEMINI_API_KEY:
    logger.error("❌ GEMINI_API_KEY não configurada!")
    raise ValueError("Configure a variável de ambiente GEMINI_API_KEY")

genai.configure(api_key=GEMINI_API_KEY)
logger.info("✅ Gemini configurado com sucesso!")

# ============================================================
# CONSTANTES
# ============================================================

BATCH_SIZE = 10  # Processar boletos em lotes
CACHE_TTL = 3600 * 24  # Cache por 24 horas
TOLERANCIA_VALOR_PERCENT = 0.02  # 2% de tolerância no match por valor

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
# FUNÇÃO: EXTRAIR VALOR DO NOME DO ARQUIVO (MELHORADA)
# ============================================================

def extrair_valor_do_nome(nome_arquivo):
    """
    Extrai valor de diversos formatos no nome do arquivo:
    - 'R$ 148,08' 
    - 'R$ 1.234,56'
    - 'R$148.08'
    - '148,08'
    - '1234.56'
    """
    # Remove extensão
    nome = nome_arquivo.replace('.pdf', '').replace('.PDF', '')
    
    # Padrões múltiplos (em ordem de especificidade)
    patterns = [
        r'R\$\s*([\d.]+,\d{2})',      # R$ 1.234,56 (formato BR com R$)
        r'R\$\s*([\d]+,\d{2})',       # R$ 1234,56 (sem pontos)
        r'R\$\s*([\d]+\.\d{2})',      # R$ 1234.56 (ponto decimal)
        r'R\$\s*([\d.]+\.\d{2})',     # R$ 1.234.56 (formato US com R$)
        r'\b([\d.]+,\d{2})\b',        # 1.234,56 solto
        r'\b([\d]+,\d{2})\b',         # 1234,56 solto
        r'\b([\d]+\.\d{2})\b',        # 1234.56 solto
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, nome)
        if matches:
            valor_str = matches[0]
            try:
                # Normalizar para float
                if ',' in valor_str and '.' in valor_str:
                    # Formato BR: 1.234,56
                    valor = float(valor_str.replace('.', '').replace(',', '.'))
                elif ',' in valor_str:
                    # 1234,56
                    valor = float(valor_str.replace(',', '.'))
                else:
                    # 1234.56
                    valor = float(valor_str)
                
                if valor > 0:
                    logger.debug(f"Valor extraído do nome '{nome_arquivo}': R$ {valor:.2f}")
                    return valor
            except Exception as e:
                logger.warning(f"Erro ao converter valor '{valor_str}': {e}")
                continue
    
    logger.debug(f"Nenhum valor encontrado no nome: {nome_arquivo}")
    return 0.0

# ============================================================
# UTILITÁRIOS: CONVERSÃO DE NÚMEROS
# ============================================================

def converter_para_virgula(valor_ou_string):
    """
    Converte número de formato com ponto para vírgula (formato brasileiro).
    """
    if not valor_ou_string:
        return "0,00"
    
    valor_str = str(valor_ou_string).strip()
    
    # Já está no formato correto
    if ',' in valor_str and '.' not in valor_str:
        return valor_str
    
    # Formato com ponto decimal
    if '.' in valor_str and ',' not in valor_str:
        partes = valor_str.split('.')
        
        # Se último componente tem 2 dígitos, é decimal
        if len(partes[-1]) == 2:
            numero_sem_pontos = valor_str.replace('.', '')
            return numero_sem_pontos[:-2] + ',' + numero_sem_pontos[-2:]
        else:
            # Pontos são separadores de milhar
            numero_sem_pontos = valor_str.replace('.', '')
            if len(numero_sem_pontos) > 2:
                return numero_sem_pontos[:-2] + ',' + numero_sem_pontos[-2:]
    
    # Tem vírgula e ponto (formato BR completo)
    if '.' in valor_str and ',' in valor_str:
        return valor_str
    
    # Apenas dígitos
    if valor_str.isdigit():
        if len(valor_str) > 2:
            return valor_str[:-2] + ',' + valor_str[-2:]
        else:
            return '0,' + valor_str.zfill(2)
    
    return valor_str


def normalizar_valor(valor):
    """Normaliza qualquer tipo de valor para float."""
    if isinstance(valor, (int, float)):
        return float(valor)
    
    if isinstance(valor, str):
        valor = valor.strip()
        valor = valor.replace('R$', '').replace(' ', '').strip()
        
        # Formato brasileiro: 1.234,56
        if ',' in valor:
            valor = valor.replace('.', '').replace(',', '.')
        
        try:
            return float(valor)
        except Exception as e:
            logger.warning(f"Erro ao normalizar valor '{valor}': {e}")
            return 0.0
    
    return 0.0

# ============================================================
# 1. EXTRAÇÃO COM GEMINI VISION (MELHORADA COM CACHE)
# ============================================================

def extrair_com_gemini(pdf_bytes_ou_caminho, use_first_page_only=True, use_cache=True):
    """
    Usa Google Gemini Vision para extrair código de barras e valor de um PDF.
    Agora com cache para evitar chamadas duplicadas.
    """
    
    try:
        # Calcular hash para cache
        if use_cache:
            if isinstance(pdf_bytes_ou_caminho, bytes):
                pdf_hash = hashlib.md5(pdf_bytes_ou_caminho).hexdigest()
            else:
                with open(pdf_bytes_ou_caminho, 'rb') as f:
                    pdf_hash = hashlib.md5(f.read()).hexdigest()
            
            cache_key = f'gemini_extract_{pdf_hash}'
            cached_result = cache.get(cache_key)
            
            if cached_result:
                logger.debug(f"✓ Cache hit para {pdf_hash[:8]}")
                return cached_result
        
        # Converter PDF para imagens
        if isinstance(pdf_bytes_ou_caminho, bytes):
            images = convert_from_bytes(pdf_bytes_ou_caminho, first_page=use_first_page_only)
        elif isinstance(pdf_bytes_ou_caminho, io.BytesIO):
            pdf_bytes_ou_caminho.seek(0)
            images = convert_from_bytes(pdf_bytes_ou_caminho.read(), first_page=use_first_page_only)
            pdf_bytes_ou_caminho.seek(0)
        else:
            images = convert_from_path(pdf_bytes_ou_caminho, first_page=use_first_page_only)
        
        if not images:
            logger.warning("Nenhuma imagem extraída do PDF")
            return {'codigo': None, 'valor': 0.0, 'valor_formatado': '0,00', 'empresa': 'N/A'}
        
        image = images[0]
        
        # Converter imagem para base64
        img_buffer = io.BytesIO()
        image.save(img_buffer, format='PNG')
        img_base64 = base64.standard_b64encode(img_buffer.getvalue()).decode('utf-8')
        
        # Prompt melhorado com exemplos
        prompt = """Analise esta imagem de um documento financeiro (boleto, comprovante ou similar) e extraia com MÁXIMA PRECISÃO:

1. CÓDIGO DE BARRAS / CÓDIGO NUMÉRICO:
   - Procure por uma sequência longa de números (geralmente 44-48 dígitos)
   - Pode estar em formato de código de barras ou linha digitável
   - Exemplos: "34191790010104351004791020150008291070026000"

2. VALOR EM REAIS:
   - Procure por valores monetários (R$, reais)
   - Retorne APENAS o número com ponto decimal (ex: 402.00, 1234.56)
   - NÃO inclua R$ ou vírgulas no valor
   - Exemplos: "402.00" (para R$ 402,00), "1234.56" (para R$ 1.234,56)

3. EMPRESA/CEDENTE/BENEFICIÁRIO:
   - Nome da empresa que emitiu o documento
   - Ou pessoa/entidade que receberá o pagamento

IMPORTANTE: Responda APENAS com JSON puro (sem markdown, sem ```):
{
  "codigo": "sequência numérica completa ou null",
  "valor": "número com ponto decimal (ex: 402.00) ou null",
  "empresa": "nome da empresa ou N/A"
}

Se não encontrar algum campo, use null (sem aspas) ou "N/A" para empresa.
"""
        
        # Chamar Gemini Vision
        model = genai.GenerativeModel('gemini-2.0-flash-exp')
        
        response = model.generate_content(
            [
                {
                    "mime_type": "image/png",
                    "data": img_base64,
                },
                prompt
            ]
        )
        
        response_text = response.text
        
        # Limpar markdown
        if "```json" in response_text:
            response_text = response_text.split("```json")[1].split("```")[0]
        elif "```" in response_text:
            response_text = response_text.split("```")[1].split("```")[0]
        
        response_text = response_text.strip()
        
        # Parse JSON
        dados = json.loads(response_text)
        
        # Converter valor para float
        valor = 0.0
        if dados.get('valor') and dados['valor'] not in ['0.00', 'null', None, 'N/A']:
            valor = normalizar_valor(dados['valor'])
        
        valor_formatado = converter_para_virgula(f"{valor:.2f}")
        
        resultado = {
            'codigo': dados.get('codigo'),
            'valor': valor,
            'valor_formatado': valor_formatado,
            'empresa': dados.get('empresa', 'N/A') or 'N/A'
        }
        
        # Salvar no cache
        if use_cache:
            cache.set(cache_key, resultado, CACHE_TTL)
        
        return resultado
    
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar JSON do Gemini: {e}")
        logger.debug(f"Resposta recebida: {response_text[:200]}")
        return {'codigo': None, 'valor': 0.0, 'valor_formatado': '0,00', 'empresa': 'N/A'}
    
    except Exception as e:
        logger.error(f"⚠️ Gemini falhou: {str(e)}", exc_info=True)
        return {'codigo': None, 'valor': 0.0, 'valor_formatado': '0,00', 'empresa': 'N/A'}

# ============================================================
# 2. TABELA TEMPORÁRIA (MELHORADA)
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
        """Busca comprovante por código de barras."""
        if not codigo:
            return None
        
        # Limpar código (remover espaços, traços)
        codigo_limpo = re.sub(r'[^0-9]', '', str(codigo))
        
        for comp in self.comprovantes:
            if comp['id'] in self.usados:
                continue
            
            if comp['codigo']:
                codigo_comp_limpo = re.sub(r'[^0-9]', '', str(comp['codigo']))
                
                # Match exato ou contém
                if codigo_limpo == codigo_comp_limpo:
                    logger.info(f"✓ Match EXATO por código: {codigo_limpo[:20]}...")
                    return comp
                elif codigo_limpo in codigo_comp_limpo or codigo_comp_limpo in codigo_limpo:
                    logger.info(f"✓ Match PARCIAL por código")
                    return comp
        
        return None
    
    def buscar_por_valor(self, valor, tolerancia_percent=TOLERANCIA_VALOR_PERCENT):
        """
        Busca comprovante por valor com tolerância percentual.
        Tolerância padrão: 2% (0.02)
        """
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
            logger.info(f"✓ Match por valor: R$ {valor:.2f} ≈ R$ {melhor_match['valor']:.2f} (diff: R$ {menor_diferenca:.2f})")
        
        return melhor_match
    
    def buscar_por_empresa_e_valor(self, empresa, valor, tolerancia_percent=TOLERANCIA_VALOR_PERCENT):
        """Match combinado: empresa + valor (mais preciso)."""
        if not empresa or empresa == 'N/A' or valor == 0:
            return None
        
        empresa_limpa = empresa.lower().strip()
        diferenca_maxima = valor * tolerancia_percent
        
        for comp in self.comprovantes:
            if comp['id'] in self.usados:
                continue
            
            comp_empresa = str(comp['empresa']).lower().strip()
            
            # Verificar se empresa está contida
            if empresa_limpa in comp_empresa or comp_empresa in empresa_limpa:
                # E valor está próximo
                if abs(comp['valor'] - valor) <= diferenca_maxima:
                    logger.info(f"✓ Match por EMPRESA + VALOR: {empresa} / R$ {valor:.2f}")
                    return comp
        
        return None
    
    def marcar_usado(self, id_comp):
        self.usados.add(id_comp)
        logger.debug(f"Comprovante {id_comp} marcado como usado")
    
    def get_stats(self):
        """Retorna estatísticas da tabela."""
        return {
            'total': len(self.comprovantes),
            'usados': len(self.usados),
            'disponiveis': len(self.comprovantes) - len(self.usados)
        }

# ============================================================
# 3. PROCESSAMENTO PRINCIPAL (MELHORADO)
# ============================================================

def processar_reconciliacao(caminho_comprovantes, lista_caminhos_boletos, user):
    """
    Processamento com Google Gemini Vision + Melhorias:
    - Cache de resultados
    - Processamento em lotes
    - Logging detalhado
    - Match inteligente (código > empresa+valor > valor)
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
            # Salvar página como PDF bytes
            writer = PdfWriter()
            writer.add_page(page)
            bio = io.BytesIO()
            writer.write(bio)
            bio.seek(0)
            
            # Usar Gemini Vision para extrair dados
            yield emit('log', f'  [Gemini] Analisando página {idx+1}/{total_paginas}...')
            
            try:
                dados_gemini = extrair_com_gemini(bio, use_cache=True)
                
                codigo = dados_gemini['codigo']
                valor = dados_gemini['valor']
                valor_formatado = dados_gemini['valor_formatado']
                empresa = dados_gemini['empresa']
                
                # Adicionar à tabela
                tabela.adicionar(
                    id_comp=idx,
                    codigo=codigo,
                    valor=valor,
                    valor_formatado=valor_formatado,
                    empresa=empresa,
                    pdf_bytes=bio
                )
                
                cod_display = (codigo[:25] + "...") if codigo else "SEM_CODIGO"
                yield emit('log', f'  ✓ Pág {idx+1}: R$ {valor_formatado} | {cod_display} | {empresa}')
                yield emit('comp_status', {'index': idx, 'msg': f'R$ {valor_formatado}'})
                
            except Exception as e:
                logger.error(f"Erro ao processar página {idx+1}: {e}")
                yield emit('log', f'  ⚠️ Erro na página {idx+1}: {str(e)[:100]}')
                # Adicionar com dados vazios para não quebrar
                tabela.adicionar(
                    id_comp=idx,
                    codigo=None,
                    valor=0.0,
                    valor_formatado='0,00',
                    empresa='N/A',
                    pdf_bytes=bio
                )
    
    except Exception as e:
        logger.error(f'Erro ao ler comprovantes: {e}', exc_info=True)
        yield emit('log', f'❌ ERRO ao ler comprovantes: {str(e)}')
        return
    
    # ========================================================
    # ETAPA 2: PROCESSAR BOLETOS (EM LOTES)
    # ========================================================
    
    yield emit('log', '')
    yield emit('log', '📑 ETAPA 2: Processando boletos com Gemini')
    yield emit('log', f'Total de boletos: {len(lista_caminhos_boletos)}')
    yield emit('log', f'Processamento em lotes de {BATCH_SIZE}')
    
    resultados = []
    
    for i, caminho_boleto in enumerate(lista_caminhos_boletos):
        nome_boleto = os.path.basename(caminho_boleto)
        
        yield emit('file_start', {'filename': nome_boleto})
        yield emit('log', f'')
        yield emit('log', f'📄 Boleto {i+1}/{len(lista_caminhos_boletos)}: {nome_boleto}')
        
        try:
            # Ler PDF do boleto
            with open(caminho_boleto, 'rb') as f:
                pdf_bytes = f.read()
            
            # Usar Gemini Vision para extrair dados do boleto
            yield emit('log', f'   [Gemini] Analisando boleto...')
            
            dados_gemini = extrair_com_gemini(pdf_bytes, use_cache=True)
            
            codigo_boleto = dados_gemini['codigo']
            valor_boleto = dados_gemini['valor']
            empresa_boleto = dados_gemini['empresa']
            
            # ⭐ FALLBACK: Se Gemini não conseguiu extrair valor, tenta do NOME DO ARQUIVO
            if valor_boleto == 0.0:
                valor_extraido = extrair_valor_do_nome(nome_boleto)
                if valor_extraido > 0:
                    valor_boleto = valor_extraido
                    yield emit('log', f'   [Fallback] Valor extraído do nome: R$ {converter_para_virgula(f"{valor_boleto:.2f}")}')
            
            valor_boleto_formatado = converter_para_virgula(f"{valor_boleto:.2f}")
            
            # Salvar boleto como bytes
            bio_boleto = io.BytesIO(pdf_bytes)
            bio_boleto.seek(0)
            
            yield emit('log', f'   → Código: {(codigo_boleto[:30] + "...") if codigo_boleto else "N/A"}')
            yield emit('log', f'   → Valor: R$ {valor_boleto_formatado}')
            yield emit('log', f'   → Empresa: {empresa_boleto}')
            
            # ====================================================
            # TENTAR MATCH (ESTRATÉGIA INTELIGENTE)
            # ====================================================
            
            comprovante_encontrado = None
            metodo_match = None
            
            # 1️⃣ Prioridade 1: CÓDIGO (mais confiável)
            if codigo_boleto:
                comp = tabela.buscar_por_codigo(codigo_boleto)
                if comp:
                    comprovante_encontrado = comp
                    metodo_match = "CÓDIGO"
                    yield emit('log', f'   ✅ MATCH por CÓDIGO (página {comp["id"]+1})')
            
            # 2️⃣ Prioridade 2: EMPRESA + VALOR (alta confiança)
            if not comprovante_encontrado and empresa_boleto != 'N/A' and valor_boleto > 0:
                comp = tabela.buscar_por_empresa_e_valor(empresa_boleto, valor_boleto)
                if comp:
                    comprovante_encontrado = comp
                    metodo_match = "EMPRESA+VALOR"
                    yield emit('log', f'   ✅ MATCH por EMPRESA + VALOR (página {comp["id"]+1})')
            
            # 3️⃣ Prioridade 3: VALOR APENAS (menor confiança)
            if not comprovante_encontrado and valor_boleto > 0:
                comp = tabela.buscar_por_valor(valor_boleto)
                if comp:
                    comprovante_encontrado = comp
                    metodo_match = "VALOR"
                    yield emit('log', f'   ⚠️ MATCH por VALOR (página {comp["id"]+1}) - Verificar manualmente')
            
            # Guardar resultado
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
                yield emit('log', f'   ❌ SEM MATCH ENCONTRADO')
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
            
            # Garbage collection a cada lote
            if (i + 1) % BATCH_SIZE == 0:
                import gc
                gc.collect()
                yield emit('log', f'   [Sistema] Lote {(i+1)//BATCH_SIZE} finalizado, memória liberada')
        
        except Exception as e:
            logger.error(f'Erro ao processar boleto {nome_boleto}: {e}', exc_info=True)
            yield emit('log', f'   ❌ ERRO: {str(e)[:100]}')
            yield emit('file_done', {'filename': nome_boleto, 'status': 'error'})
            continue
    
    # ========================================================
    # ETAPA 3: GERAR ZIP
    # ========================================================
    
    yield emit('log', '')
    yield emit('log', '💾 ETAPA 3: Gerando arquivo ZIP')
    
    output_zip = io.BytesIO()
    
    try:
        with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            
            for resultado in resultados:
                nome_boleto = resultado['boleto_nome']
                
                try:
                    writer_final = PdfWriter()
                    
                    # Adicionar boleto
                    resultado['boleto_pdf'].seek(0)
                    reader_boleto = PdfReader(resultado['boleto_pdf'])
                    for page in reader_boleto.pages:
                        writer_final.add_page(page)
                    
                    # Adicionar comprovante se houver
                    if resultado['comprovante']:
                        resultado['comprovante']['pdf_bytes'].seek(0)
                        reader_comp = PdfReader(resultado['comprovante']['pdf_bytes'])
                        for page in reader_comp.pages:
                            writer_final.add_page(page)
                    
                    # Salvar no ZIP
                    bio_final = io.BytesIO()
                    writer_final.write(bio_final)
                    bio_final.seek(0)
                    
                    zip_file.writestr(nome_boleto, bio_final.getvalue())
                
                except Exception as e:
                    logger.error(f'Erro ao gerar {nome_boleto}: {e}')
                    yield emit('log', f'   ❌ ERRO ao gerar {nome_boleto}: {str(e)[:100]}')
                    continue
    
    except Exception as e:
        logger.error(f'Erro ao criar ZIP: {e}', exc_info=True)
        yield emit('log', f'❌ ERRO ao criar ZIP: {str(e)}')
        return
    
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
    
    # Estatísticas
    total_boletos = len(resultados)
    total_matches = len([r for r in resultados if r['comprovante']])
    total_sem_match = total_boletos - total_matches
    
    # Estatísticas por método
    matches_codigo = len([r for r in resultados if r.get('metodo') == 'CÓDIGO'])
    matches_empresa_valor = len([r for r in resultados if r.get('metodo') == 'EMPRESA+VALOR'])
    matches_valor = len([r for r in resultados if r.get('metodo') == 'VALOR'])
    
    stats_tabela = tabela.get_stats()
    
    yield emit('log', '')
    yield emit('log', '✅ PROCESSO CONCLUÍDO!')
    yield emit('log', f'📊 RESUMO:')
    yield emit('log', f'   Total de boletos: {total_boletos}')
    yield emit('log', f'   ✓ Matches encontrados: {total_matches}')
    yield emit('log', f'     - Por código: {matches_codigo}')
    yield emit('log', f'     - Por empresa+valor: {matches_empresa_valor}')
    yield emit('log', f'     - Por valor: {matches_valor} ⚠️')
    yield emit('log', f'   ❌ Sem match: {total_sem_match}')
    yield emit('log', f'')
    yield emit('log', f'   Comprovantes: {stats_tabela["total"]} no arquivo')
    yield emit('log', f'   Comprovantes usados: {stats_tabela["usados"]}')
    yield emit('log', f'   Comprovantes não usados: {stats_tabela["disponiveis"]}')
    yield emit('log', f'📦 Arquivo gerado: {nome_zip}')
    
    logger.info(f"Processamento concluído: {total_matches}/{total_boletos} matches - User: {user}")
    
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