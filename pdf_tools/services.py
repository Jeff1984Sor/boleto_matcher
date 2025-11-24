import io
import os
import zipfile
import time
import uuid
import json
import re
from datetime import datetime
from pypdf import PdfReader, PdfWriter
import google.generativeai as genai
from django.conf import settings

# Fallback para unidecode
try:
    from unidecode import unidecode
except ImportError:
    def unidecode(t): return t

genai.configure(api_key=settings.GOOGLE_API_KEY)

# --- CLASSE PARA A TABELA VIRTUAL ---
class ItemFinanceiro:
    def __init__(self, tipo, origem, texto, pdf_bytes, dados_ia=None):
        self.tipo = tipo
        self.origem = origem
        self.texto = texto
        self.pdf_bytes = pdf_bytes
        self.usado = False
        self.par = None
        self.match_tipo = ""
        
        # Dados Normalizados
        self.valor = 0.0
        self.data = ""
        self.codigo = ""
        self.empresa = ""
        
        if dados_ia:
            self.processar_dados(dados_ia)
            
    def processar_dados(self, dados):
        try: self.valor = float(dados.get('valor') or 0)
        except: self.valor = 0.0
        
        self.codigo = re.sub(r'\D', '', str(dados.get('codigo') or ""))
        
        raw_emp = str(dados.get('empresa') or "").upper()
        if any(x in raw_emp for x in ['PMSP', 'PREFEITURA', 'MUNICIPIO', 'SAO PAULO', 'DARF', 'RECEITA']):
            self.empresa = "GOVERNO"
        else:
            self.empresa = unidecode(raw_emp)
            
        raw_data = str(dados.get('data') or "")
        match_data = re.search(r'(\d{2}/\d{2}/\d{4})', raw_data)
        if match_data:
            self.data = match_data.group(1)

# --- EXTRAÇÃO DE EMERGÊNCIA (Salva-Vidas) ---

def extrair_valor_do_nome(nome_arquivo):
    """
    Se o PDF falhar, tenta ler o valor escrito no nome do arquivo.
    Ex: '... R$ 402_00 ...' -> 402.00
    """
    # Procura padrões como 402_00 ou 402,00
    match = re.search(r'R\$\s?(\d+)[_,.](\d{2})', nome_arquivo)
    if match:
        try:
            return float(f"{match.group(1)}.{match.group(2)}")
        except: pass
    return 0.0

def extrair_tudo_ia(texto, tipo_doc):
    model = genai.GenerativeModel('gemini-2.0-flash')
    prompt = f"""
    Analise este {tipo_doc}. Extraia JSON:
    {{ 
        "valor": float, 
        "data": "DD/MM/AAAA",
        "codigo": "string", 
        "empresa": "string" 
    }}
    Texto: {texto[:4000]}
    """
    for _ in range(2):
        try:
            resp = model.generate_content(prompt)
            clean = resp.text.replace('```json', '').replace('```', '').strip()
            return json.loads(clean)
        except: time.sleep(0.5)
    return {}

def extrair_backup_regex(texto):
    """Procura agressivamente por valores monetários."""
    dados = {}
    # Pega qualquer coisa que pareça dinheiro: 1.000,00 ou 402,00
    valores = re.findall(r'(\d{1,3}(?:\.?\d{3})*,\d{2})', texto)
    floats = []
    for v in valores:
        try: floats.append(float(v.replace('.','').replace(',','.')))
        except: pass
    
    if floats:
        # Pega o maior valor da página (geralmente é o Total)
        dados['valor'] = max(floats)
    
    datas = re.findall(r'\d{2}/\d{2}/\d{4}', texto)
    if datas: dados['data'] = datas[0]
    
    return dados

# --- LÓGICA PRINCIPAL ---

def processar_conciliacao_json_stream(lista_caminhos_boletos, caminho_comprovantes, user):
    
    def send(type, data):
        return json.dumps({'type': type, 'data': data}) + "\n"

    yield send('init_list', {'files': [os.path.basename(p) for p in lista_caminhos_boletos]})
    yield send('log', '🚀 Iniciando Tabela Virtual e Indexação...')

    tabela_comprovantes = []
    
    # 1. POPULAR A TABELA VIRTUAL (COMPROVANTES)
    yield send('log', '📂 Lendo Comprovantes...')
    reader_comp = PdfReader(caminho_comprovantes)
    total_paginas = len(reader_comp.pages)
    
    for i, page in enumerate(reader_comp.pages):
        texto = page.extract_text() or ""
        dados_ia = extrair_tudo_ia(texto, "comprovante bancario")
        
        # Fallback Regex
        if not dados_ia.get('valor'):
            backup = extrair_backup_regex(texto)
            if backup.get('valor'): dados_ia['valor'] = backup['valor']
            if backup.get('data') and not dados_ia.get('data'): dados_ia['data'] = backup['data']

        writer = PdfWriter()
        writer.add_page(page)
        bio = io.BytesIO()
        writer.write(bio)
        
        item = ItemFinanceiro('comprovante', f"Pag {i+1}", texto, bio, dados_ia)
        tabela_comprovantes.append(item)
        yield send('comp_status', {'index': i, 'msg': f"R${item.valor}"})

    # 2. LER BOLETOS E MATCH IMEDIATO
    yield send('log', '⚡ Lendo Boletos e Conciliando...')
    lista_boletos_obj = []
    
    for path in lista_caminhos_boletos:
        nome_arq = os.path.basename(path)
        yield send('file_start', {'filename': nome_arq})
        
        reader = PdfReader(path)
        texto = ""
        for p in reader.pages: texto += p.extract_text() or ""
        total_paginas += len(reader.pages)
        
        dados_ia = extrair_tudo_ia(texto, "boleto cobranca ou imposto")
        
        # 1. Backup Regex
        if not dados_ia.get('valor'):
            backup = extrair_backup_regex(texto)
            if backup.get('valor'): dados_ia['valor'] = backup['valor']
            
        # 2. Backup FILENAME (Salva-Vidas para PMSP)
        # Se ainda for 0, tenta ler do nome do arquivo
        val_final = float(dados_ia.get('valor') or 0)
        if val_final == 0:
            val_nome = extrair_valor_do_nome(nome_arq)
            if val_nome > 0:
                dados_ia['valor'] = val_nome
                yield send('log', f"   💡 Valor recuperado do nome do arquivo: R${val_nome}")

        with open(path, 'rb') as f: bio = io.BytesIO(f.read())
        boleto = ItemFinanceiro('boleto', nome_arq, texto, bio, dados_ia)
        lista_boletos_obj.append(boleto)

        # --- MATCH IMEDIATO ---
        match_encontrado = False
        
        # A. Código de Barras
        if boleto.codigo and len(boleto.codigo) > 20:
            for comp in tabela_comprovantes:
                if comp.usado: continue
                if boleto.codigo == comp.codigo or boleto.codigo.startswith(comp.codigo[:20]) or comp.codigo.startswith(boleto.codigo[:20]):
                    boleto.par = comp
                    comp.usado = True
                    boleto.match_tipo = "CÓDIGO"
                    match_encontrado = True
                    break
        
        # B. Valor + Data
        if not match_encontrado and boleto.valor > 0 and boleto.data:
            for comp in tabela_comprovantes:
                if comp.usado: continue
                if abs(boleto.valor - comp.valor) < 0.05 and boleto.data == comp.data:
                    boleto.par = comp
                    comp.usado = True
                    boleto.match_tipo = f"VALOR+DATA ({boleto.data})"
                    match_encontrado = True
                    break

        # C. Valor + Empresa
        if not match_encontrado and boleto.valor > 0:
            for comp in tabela_comprovantes:
                if comp.usado: continue
                if abs(boleto.valor - comp.valor) < 0.05:
                    n1 = boleto.empresa
                    n2 = comp.empresa
                    match_nome = False
                    if n1 == "GOVERNO" and n2 == "GOVERNO": match_nome = True
                    elif n1 and n2 and (n1 in n2 or n2 in n1): match_nome = True
                    
                    if match_nome:
                        boleto.par = comp
                        comp.usado = True
                        boleto.match_tipo = "VALOR+EMPRESA"
                        match_encontrado = True
                        break

        status = 'success' if match_encontrado else 'warning'
        yield send('file_done', {'filename': nome_arq, 'status': status})

    # 3. REPESCAGEM (SEQUENCIAL POR VALOR)
    yield send('log', '🔄 Iniciando Repescagem (Fila por Valor)...')
    
    boletos_pendentes = [b for b in lista_boletos_obj if not b.par]
    
    for boleto in boletos_pendentes:
        if boleto.valor == 0:
            yield send('log', f"   ⚠️ Ignorado (Valor Zero): {boleto.origem}")
            continue
            
        # Pega o primeiro comprovante livre com o mesmo valor
        for comp in tabela_comprovantes:
            if comp.usado: continue
            
            if abs(boleto.valor - comp.valor) < 0.05:
                boleto.par = comp
                comp.usado = True
                boleto.match_tipo = "REPESCAGEM (Valor)"
                yield send('log', f"   🔗 Match Repescagem: {boleto.origem}")
                yield send('file_done', {'filename': boleto.origem, 'status': 'success'})
                break

    # 4. GERAÇÃO
    yield send('log', '💾 Gerando Zip Final...')
    output_zip_buffer = io.BytesIO()
    with zipfile.ZipFile(output_zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for bol in lista_boletos_obj:
            writer_final = PdfWriter()
            bol.pdf_bytes.seek(0)
            rb = PdfReader(bol.pdf_bytes)
            for p in rb.pages: writer_final.add_page(p)
            
            if bol.par:
                bol.par.pdf_bytes.seek(0)
                rc = PdfReader(bol.par.pdf_bytes)
                writer_final.add_page(rc.pages[0])
            
            bio = io.BytesIO()
            writer_final.write(bio)
            zip_file.writestr(bol.origem, bio.getvalue())

    # Finaliza
    pasta = os.path.join(settings.MEDIA_ROOT, 'downloads')
    os.makedirs(pasta, exist_ok=True)
    nome_zip = f"Conciliacao_Final_{uuid.uuid4().hex[:8]}.zip"
    with open(os.path.join(pasta, nome_zip), "wb") as f:
        f.write(output_zip_buffer.getvalue())

    if hasattr(user, 'paginas_processadas'):
        user.paginas_processadas += total_paginas
        user.save()

    yield send('finish', {'url': f"{settings.MEDIA_URL}downloads/{nome_zip}", 'total': total_paginas})