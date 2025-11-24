import io
import os
import zipfile
import time
import uuid
import json
import re
from dataclasses import dataclass
from typing import List, Optional
from pypdf import PdfReader, PdfWriter
import google.generativeai as genai
from django.conf import settings

genai.configure(api_key=settings.GOOGLE_API_KEY)

# --- ESTRUTURA DE DADOS ("TABELA TEMPORÁRIA") ---
@dataclass
class DocumentoItem:
    id: str
    tipo: str  # 'boleto' ou 'comprovante'
    origem: str # Nome do arquivo ou indice da pagina
    texto_bruto: str
    pdf_bytes: io.BytesIO # O binário do PDF (pagina unica ou arquivo inteiro)
    
    # Dados extraídos
    valor: float = 0.0
    codigo_barras: str = ""
    codigo_limpo: str = ""
    
    # Controle
    resolvido: bool = False
    par_encontrado: Optional['DocumentoItem'] = None

# --- FUNÇÕES UTILITÁRIAS ---

def limpar_apenas_numeros(texto):
    """Remove tudo que não for dígito 0-9."""
    if not texto: return ""
    return re.sub(r'\D', '', str(texto))

def extrair_regex_seguro(texto):
    """Tenta achar sequência de 44 a 48 digitos no texto bruto."""
    texto_sem_formatacao = texto.replace('\n', '').replace(' ', '').replace('.', '').replace('-', '')
    match = re.search(r'\d{44,48}', texto_sem_formatacao)
    return match.group(0) if match else None

def chamar_ai(texto, modelo, tipo_doc):
    """Chama a IA (Flash ou Pro)"""
    model = genai.GenerativeModel(modelo)
    prompt = f"""
    Analise este texto de {tipo_doc}. Retorne JSON {{ "valor": float, "codigo_barras": "string" }}.
    Priorize encontrar a linha digitável ou código de barras numérico (44-48 digitos).
    Texto: {texto[:6000]}
    """
    for _ in range(3):
        try:
            resp = model.generate_content(prompt)
            clean = resp.text.replace('```json', '').replace('```', '').strip()
            return json.loads(clean)
        except:
            time.sleep(2)
    return {}

# --- MOTOR PRINCIPAL ---

def processar_conciliacao_json_stream(lista_caminhos_boletos, caminho_comprovantes, user):
    
    def send(type, data):
        return json.dumps({'type': type, 'data': data}) + "\n"

    yield send('log', '🚀 Iniciando arquitetura em 5 fases...')

    # Tabelas em memória
    tb_boletos: List[DocumentoItem] = []
    tb_comprovantes: List[DocumentoItem] = []
    total_paginas = 0

    # ====================================================
    # FASE 1: INGESTÃO E LEITURA RÁPIDA (REGEX + FLASH)
    # ====================================================
    yield send('log', '📂 FASE 1: Leitura Inicial dos Arquivos...')

    # 1.1 Ingestão Comprovantes (página por página)
    reader_comp = PdfReader(caminho_comprovantes)
    for i, page in enumerate(reader_comp.pages):
        yield send('comp_status', {'index': i, 'msg': 'Lendo...'})
        
        texto = page.extract_text() or ""
        total_paginas += 1
        
        # Cria PDF unitário em memória
        writer = PdfWriter()
        writer.add_page(page)
        buffer = io.BytesIO()
        writer.write(buffer)
        buffer.seek(0)
        
        item = DocumentoItem(
            id=str(uuid.uuid4()),
            tipo='comprovante',
            origem=f"Pagina {i+1}",
            texto_bruto=texto,
            pdf_bytes=buffer
        )
        
        # Tenta Regex primeiro (Custo Zero)
        cod_regex = extrair_regex_seguro(texto)
        if cod_regex:
            item.codigo_limpo = cod_regex
            # Se achou regex, pega valor via IA rapida (opcional, ou regex de valor)
            dados_ai = chamar_ai(texto, 'gemini-2.0-flash', 'comprovante')
            item.valor = float(dados_ai.get('valor') or 0)
        else:
            # Se não achou regex, tenta IA Flash
            dados_ai = chamar_ai(texto, 'gemini-2.0-flash', 'comprovante')
            item.valor = float(dados_ai.get('valor') or 0)
            item.codigo_barras = dados_ai.get('codigo_barras', '')
            item.codigo_limpo = limpar_apenas_numeros(item.codigo_barras)

        tb_comprovantes.append(item)

    # 1.2 Ingestão Boletos
    for path in lista_caminhos_boletos:
        nome = os.path.basename(path)
        yield send('file_start', {'filename': nome})
        
        reader = PdfReader(path)
        texto = ""
        for p in reader.pages: texto += p.extract_text() + "\n"
        total_paginas += len(reader.pages)
        
        # Lê o binário do arquivo original
        with open(path, 'rb') as f:
            pdf_bytes = io.BytesIO(f.read())

        item = DocumentoItem(
            id=str(uuid.uuid4()),
            tipo='boleto',
            origem=nome,
            texto_bruto=texto,
            pdf_bytes=pdf_bytes
        )

        cod_regex = extrair_regex_seguro(texto)
        if cod_regex:
            item.codigo_limpo = cod_regex
            dados_ai = chamar_ai(texto, 'gemini-2.0-flash', 'boleto')
            item.valor = float(dados_ai.get('valor') or 0)
        else:
            dados_ai = chamar_ai(texto, 'gemini-2.0-flash', 'boleto')
            item.valor = float(dados_ai.get('valor') or 0)
            item.codigo_barras = dados_ai.get('codigo_barras', '')
            item.codigo_limpo = limpar_apenas_numeros(item.codigo_barras)
            
        tb_boletos.append(item)
        yield send('file_done', {'filename': nome, 'status': 'info'})

    # ====================================================
    # FASE 2: MATCH PRIMÁRIO (CÓDIGO EXATO)
    # ====================================================
    yield send('log', '⚡ FASE 2: Cruzamento Rápido...')
    
    match_count = 0
    for bol in tb_boletos:
        if bol.resolvido or not bol.codigo_limpo: continue
        
        for comp in tb_comprovantes:
            if comp.resolvido or not comp.codigo_limpo: continue
            
            # Match exato ou parcial longo
            if bol.codigo_limpo == comp.codigo_limpo or \
               (len(bol.codigo_limpo) > 20 and bol.codigo_limpo.startswith(comp.codigo_limpo[:20])):
                
                bol.resolvido = True
                bol.par_encontrado = comp
                comp.resolvido = True
                match_count += 1
                break
    
    yield send('log', f'   > {match_count} pares encontrados na fase rápida.')

    # ====================================================
    # FASE 3: REPESCAGEM COM IA PRO (SÓ OS FALHOS)
    # ====================================================
    # Filtra quem sobrou
    boletos_pendentes = [b for b in tb_boletos if not b.resolvido]
    comps_pendentes = [c for c in tb_comprovantes if not c.resolvido]

    if boletos_pendentes and comps_pendentes:
        yield send('log', f'🧠 FASE 3: IA Avançada (Gemini Pro) em {len(boletos_pendentes)} boletos e {len(comps_pendentes)} comprovantes...')
        
        # Melhora dados dos Boletos Pendentes
        for bol in boletos_pendentes:
            yield send('log', f'   > Analisando profundamente: {bol.origem}')
            dados_pro = chamar_ai(bol.texto_bruto, 'gemini-1.5-pro', 'boleto difícil')
            
            novo_cod = limpar_apenas_numeros(dados_pro.get('codigo_barras'))
            if len(novo_cod) > len(bol.codigo_limpo): # Se achou um código melhor
                bol.codigo_limpo = novo_cod
            if dados_pro.get('valor'):
                bol.valor = float(dados_pro.get('valor'))
            time.sleep(2) # Respeita cota

        # Melhora dados dos Comprovantes Pendentes
        for comp in comps_pendentes:
            yield send('log', f'   > Analisando profundamente: {comp.origem}')
            dados_pro = chamar_ai(comp.texto_bruto, 'gemini-1.5-pro', 'comprovante difícil')
            
            novo_cod = limpar_apenas_numeros(dados_pro.get('codigo_barras'))
            if len(novo_cod) > len(comp.codigo_limpo):
                comp.codigo_limpo = novo_cod
            if dados_pro.get('valor'):
                comp.valor = float(dados_pro.get('valor'))
            time.sleep(2)

    # ====================================================
    # FASE 4: MATCH FINAL (CÓDIGO + VALOR)
    # ====================================================
    yield send('log', '🔍 FASE 4: Cruzamento Final...')
    
    for bol in tb_boletos:
        if bol.resolvido: continue
        
        # Tenta match com os comprovantes que ainda estão livres
        for comp in tb_comprovantes:
            if comp.resolvido: continue
            
            match_found = False
             motivo = ""
            
            # 1. Tenta Código de novo (agora com dados da IA Pro)
            if bol.codigo_limpo and comp.codigo_limpo:
                 if bol.codigo_limpo == comp.codigo_limpo or \
                   (len(bol.codigo_limpo) > 20 and bol.codigo_limpo.startswith(comp.codigo_limpo[:20])):
                    match_found = True
                    motivo = "Código (Pro)"

            # 2. Tenta Valor (Último recurso)
            if not match_found and bol.valor > 0 and comp.valor > 0:
                if abs(bol.valor - comp.valor) < 0.05: # 5 centavos
                    match_found = True
                    motivo = "Valor"
            
            if match_found:
                bol.resolvido = True
                bol.par_encontrado = comp
                comp.resolvido = True
                yield send('log', f'   ✅ Match recuperado ({motivo}): {bol.origem}')
                break
        
        if not bol.resolvido:
            yield send('log', f'   ❌ Não foi possível conciliar: {bol.origem}')

    # ====================================================
    # FASE 5: GERAÇÃO DO ZIP FINAL
    # ====================================================
    yield send('log', '💾 FASE 5: Gerando arquivos...')
    
    output_zip_buffer = io.BytesIO()
    with zipfile.ZipFile(output_zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for bol in tb_boletos:
            writer_final = PdfWriter()
            
            # Adiciona boleto
            bol.pdf_bytes.seek(0)
            reader_bol = PdfReader(bol.pdf_bytes)
            for p in reader_bol.pages: writer_final.add_page(p)
            
            # Adiciona comprovante (se houver)
            status_final = 'warning'
            if bol.par_encontrado:
                status_final = 'success'
                bol.par_encontrado.pdf_bytes.seek(0)
                reader_comp = PdfReader(bol.par_encontrado.pdf_bytes)
                writer_final.add_page(reader_comp.pages[0])
            
            # Salva
            pdf_out = io.BytesIO()
            writer_final.write(pdf_out)
            zip_file.writestr(bol.origem, pdf_out.getvalue())
            
            # Atualiza UI final
            yield send('file_done', {'filename': bol.origem, 'status': status_final})

    # Finaliza processo
    pasta_downloads = os.path.join(settings.MEDIA_ROOT, 'downloads')
    os.makedirs(pasta_downloads, exist_ok=True)
    nome_zip = f"Conciliacao_Pro_{uuid.uuid4().hex[:8]}.zip"
    
    with open(os.path.join(pasta_downloads, nome_zip), "wb") as f:
        f.write(output_zip_buffer.getvalue())

    if hasattr(user, 'paginas_processadas'):
        user.paginas_processadas += total_paginas
        user.save()

    yield send('finish', {
        'url': f"{settings.MEDIA_URL}downloads/{nome_zip}", 
        'total': total_paginas
    })