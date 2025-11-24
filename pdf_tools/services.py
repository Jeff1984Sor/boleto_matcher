"""
PROJETO: Reconciliação de Boletos com Comprovantes
Fluxo:
1. Ler PDF comprovantes (múltiplas páginas)
2. Extrair código, valor, empresa de CADA página
3. Montar tabela temporária
4. Ler boletos (arquivos separados)
5. Procurar cada boleto na tabela
6. Se achou = gerar PDF junção (boleto + comprovante)
"""

import io
import os
import re
import zipfile
import uuid
import json
from pypdf import PdfReader, PdfWriter
from django.conf import settings

# ============================================================
# 1. EXTRAÇÃO DE DADOS
# ============================================================

def extrair_codigo_barras(texto):
    """
    Extrai código de barras de um texto.
    Procura por sequência de números com 47-50 dígitos (padrão de boleto).
    """
    # Remove quebras de linha e espaços
    texto_limpo = texto.replace('\n', ' ').replace('\r', ' ')
    
    # Busca códigos de barras: sequência longa de números
    matches = re.findall(r'\b\d{40,55}\b', texto_limpo)
    
    if matches:
        # Retorna o primeiro (normalmente o mais longo)
        return matches[0]
    
    return None

def extrair_valor(texto):
    """
    Extrai valor em formato R$ XXX,XX ou XXX.XXX,XX
    """
    # Padrão: R$ 1.234,56 ou R$ 1234,56
    matches = re.findall(r'R\$\s*[\d.]*\d+,\d{2}', texto)
    
    if matches:
        valor_str = matches[0]
        # Remove 'R$' e espaços, depois converte
        valor_str = valor_str.replace('R$', '').strip()
        valor_str = valor_str.replace('.', '').replace(',', '.')
        try:
            return float(valor_str)
        except:
            return 0.0
    
    return 0.0

def extrair_empresa(texto):
    """
    Tenta extrair nome da empresa/cedente do texto.
    Procura por padrões como "Nome:" ou "Cedente:"
    """
    # Procura por linhas com "Nome:" ou "Cedente:"
    linhas = texto.split('\n')
    
    for i, linha in enumerate(linhas):
        if 'Nome:' in linha or 'NOME:' in linha:
            partes = linha.split(':', 1)
            if len(partes) > 1:
                empresa = partes[1].strip()[:50]  # Pega até 50 caracteres
                if empresa:
                    return empresa
    
    return "N/A"

# ============================================================
# 2. TABELA TEMPORÁRIA DE COMPROVANTES
# ============================================================

class TabelaComprovantes:
    """
    Tabela temporária para armazenar dados dos comprovantes.
    """
    def __init__(self):
        self.comprovantes = []  # Lista de dicts
        self.usados = set()  # IDs dos comprovantes já usados
    
    def adicionar(self, id_comp, codigo, valor, empresa, pdf_bytes):
        """
        Adiciona um comprovante à tabela.
        """
        item = {
            'id': id_comp,
            'codigo': codigo,
            'valor': valor,
            'empresa': empresa,
            'pdf_bytes': pdf_bytes,
        }
        self.comprovantes.append(item)
        
        return item
    
    def buscar_por_codigo(self, codigo):
        """
        Procura um comprovante por código de barras.
        Retorna o primeiro que não foi usado.
        """
        if not codigo:
            return None
        
        for comp in self.comprovantes:
            if comp['id'] in self.usados:
                continue
            
            # Comparação: se um contém o outro ou são iguais
            if comp['codigo'] and codigo in comp['codigo']:
                return comp
            if comp['codigo'] and comp['codigo'] in codigo:
                return comp
        
        return None
    
    def buscar_por_valor(self, valor, tolerancia=0.05):
        """
        Procura um comprovante por valor.
        Com tolerância de R$ 0.05.
        """
        if valor == 0:
            return None
        
        for comp in self.comprovantes:
            if comp['id'] in self.usados:
                continue
            
            if abs(comp['valor'] - valor) < tolerancia:
                return comp
        
        return None
    
    def marcar_usado(self, id_comp):
        """
        Marca um comprovante como usado.
        """
        self.usados.add(id_comp)
    
    def listar_nao_usados(self):
        """
        Retorna lista de comprovantes não usados.
        """
        return [c for c in self.comprovantes if c['id'] not in self.usados]

# ============================================================
# 3. FUNÇÃO PRINCIPAL DE PROCESSAMENTO
# ============================================================

def processar_reconciliacao(caminho_comprovantes, lista_caminhos_boletos, user):
    """
    Função principal que executa todo o fluxo.
    Retorna um GENERATOR que yield eventos NDJSON.
    """
    
    def emit(tipo, dados):
        """Emite evento NDJSON."""
        return json.dumps({'type': tipo, 'data': dados}) + "\n"
    
    # ========================================================
    # ETAPA 1: CARREGAR COMPROVANTES
    # ========================================================
    
    yield emit('log', '🚀 Iniciando processamento...')
    yield emit('log', '📋 ETAPA 1: Lendo arquivo de comprovantes')
    
    tabela = TabelaComprovantes()
    
    try:
        reader_comp = PdfReader(caminho_comprovantes)
        total_paginas = len(reader_comp.pages)
        
        yield emit('log', f'📄 Total de páginas: {total_paginas}')
        
        for idx, page in enumerate(reader_comp.pages):
            texto = page.extract_text() or ""
            
            codigo = extrair_codigo_barras(texto)
            valor = extrair_valor(texto)
            empresa = extrair_empresa(texto)
            
            # Salvar página como PDF bytes
            writer = PdfWriter()
            writer.add_page(page)
            bio = io.BytesIO()
            writer.write(bio)
            bio.seek(0)  # ✅ IMPORTANTE: resetar para início
            
            # Adicionar à tabela
            item = tabela.adicionar(
                id_comp=idx,
                codigo=codigo,
                valor=valor,
                empresa=empresa,
                pdf_bytes=bio
            )
            
            # Log com formatação amigável
            cod_display = codigo[:25] + "..." if codigo else "SEM_CODIGO"
            yield emit('log', f'  ✓ Pág {idx+1}: R$ {valor:.2f} | {cod_display} | {empresa}')
            yield emit('comp_status', {'index': idx, 'msg': f'R$ {valor:.2f}'})
    
    except Exception as e:
        yield emit('log', f'❌ ERRO ao ler comprovantes: {str(e)}')
        return
    
    # ========================================================
    # ETAPA 2: PROCESSAR BOLETOS
    # ========================================================
    
    yield emit('log', '')  # Linha em branco
    yield emit('log', '📑 ETAPA 2: Processando boletos')
    yield emit('log', f'Total de boletos: {len(lista_caminhos_boletos)}')
    
    resultados = []  # Armazenar pares boleto + comprovante
    
    for i, caminho_boleto in enumerate(lista_caminhos_boletos):
        nome_boleto = os.path.basename(caminho_boleto)
        
        # Signal para o frontend que começou
        yield emit('file_start', {'filename': nome_boleto})
        yield emit('log', f'')
        yield emit('log', f'📄 Boleto {i+1}/{len(lista_caminhos_boletos)}: {nome_boleto}')
        
        try:
            # Ler boleto
            reader_boleto = PdfReader(caminho_boleto)
            texto_boleto = ""
            for page in reader_boleto.pages:
                texto_boleto += page.extract_text() or ""
            
            codigo_boleto = extrair_codigo_barras(texto_boleto)
            valor_boleto = extrair_valor(texto_boleto)
            
            # Salvar boleto como bytes
            with open(caminho_boleto, 'rb') as f:
                bio_boleto = io.BytesIO(f.read())
                bio_boleto.seek(0)  # ✅ IMPORTANTE: resetar
            
            yield emit('log', f'   Código: {codigo_boleto[:30] if codigo_boleto else "N/A"}')
            yield emit('log', f'   Valor: R$ {valor_boleto:.2f}')
            
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
            
            # Marcar como usado e guardar resultado
            status = 'warning'  # Padrão: sem match
            if comprovante_encontrado:
                tabela.marcar_usado(comprovante_encontrado['id'])
                status = 'success'
                resultados.append({
                    'boleto_nome': nome_boleto,
                    'boleto_codigo': codigo_boleto,
                    'boleto_valor': valor_boleto,
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
                    'boleto_pdf': bio_boleto,
                    'comprovante': None,
                    'metodo': None
                })
            
            # Signal para o frontend que terminou
            yield emit('file_done', {'filename': nome_boleto, 'status': status})
        
        except Exception as e:
            yield emit('log', f'   ❌ ERRO: {str(e)}')
            yield emit('file_done', {'filename': nome_boleto, 'status': 'error'})
            continue
    
    # ========================================================
    # ETAPA 3: GERAR ARQUIVOS FINAIS
    # ========================================================
    
    yield emit('log', '')
    yield emit('log', '💾 ETAPA 3: Gerando arquivo ZIP')
    
    output_zip = io.BytesIO()
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        
        for resultado in resultados:
            nome_boleto = resultado['boleto_nome']
            
            try:
                # Criar novo PDF com boleto + comprovante
                writer_final = PdfWriter()
                
                # Adicionar boleto
                resultado['boleto_pdf'].seek(0)
                reader_boleto = PdfReader(resultado['boleto_pdf'])
                for page in reader_boleto.pages:
                    writer_final.add_page(page)
                
                # Adicionar comprovante (se encontrou)
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
                yield emit('log', f'   ❌ ERRO ao gerar {nome_boleto}: {str(e)}')
                continue
    
    # ========================================================
    # SALVAR ZIP NO SERVIDOR
    # ========================================================
    
    pasta_downloads = os.path.join(settings.MEDIA_ROOT, 'downloads')
    os.makedirs(pasta_downloads, exist_ok=True)
    
    nome_zip = f"Reconciliacao_{uuid.uuid4().hex[:8]}.zip"
    caminho_zip = os.path.join(pasta_downloads, nome_zip)
    
    with open(caminho_zip, 'wb') as f:
        f.write(output_zip.getvalue())
    
    url_download = f"{settings.MEDIA_URL}downloads/{nome_zip}"
    
    # ========================================================
    # RELATÓRIO FINAL
    # ========================================================
    
    total_boletos = len(resultados)
    total_matches = len([r for r in resultados if r['comprovante']])
    total_sem_match = total_boletos - total_matches
    
    yield emit('log', '')
    yield emit('log', '✅ PROCESSO CONCLUÍDO!')
    yield emit('log', f'📊 RESUMO:')
    yield emit('log', f'   Total de boletos: {total_boletos}')
    yield emit('log', f'   Encontrados: {total_matches}')
    yield emit('log', f'   Sem match: {total_sem_match}')
    yield emit('log', f'📦 Arquivo gerado com sucesso!')
    
    # Retornar resultado final (trigger para mostrar botão de download)
    yield emit('finish', {
        'url': url_download,
        'total': total_boletos,
        'matches': total_matches,
        'sem_match': total_sem_match
    })