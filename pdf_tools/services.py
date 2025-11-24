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

# Fallback para unidecode
try:
    from unidecode import unidecode
except ImportError:
    def unidecode(t): return t

genai.configure(api_key=settings.GOOGLE_API_KEY)

# --- ESTRUTURA DE DADOS ---

@dataclass
class Documento:
    id: str
    nome_arquivo: str # Para boleto é filename, para comprovante é "Pg X"
    texto: str
    pdf_object: io.BytesIO
    
    # Dados extraídos
    valor: float = 0.0
    codigo: str = ""
    empresa: str = ""
    
    # Estado
    conciliado_com: Optional['Documento'] = None
    metodo_match: str = ""

# --- FERRAMENTAS DE EXTRAÇÃO ---

def limpar_digitos(t):
    return re.sub(r'\D', '', str(t or ""))

def normalizar_empresa(nome):
    if not nome: return ""
    nome = nome.upper().replace('.', ' ').replace('-', ' ').replace('/', ' ')
    try: nome = unidecode(nome)
    except: pass
    
    # Normalização agressiva para Governo
    if any(x in nome for x in ['PMSP', 'PREFEITURA', 'MUNICIPIO', 'SAO PAULO', 'RECEITA', 'FEDERAL', 'DARF']):
        return "GOVERNO"
    
    ignorar = ['LTDA', 'S.A.', 'BANCO', 'PAGAMENTO', 'BOLETO', 'BENEFICIARIO']
    palavras = [p for p in nome.split() if p not in ignorar and len(p) > 2]
    return " ".join(palavras)

def extrair_dados_ia(texto, tipo_doc):
    """Usa IA para extrair tudo de uma vez."""
    model = genai.GenerativeModel('gemini-2.0-flash')
    prompt = f"""
    Analise este {tipo_doc}. Extraia JSON: {{ "valor": float, "codigo": "string", "empresa": "string" }}
    DICA: Para DAMSP/Prefeitura, o valor geralmente está no campo "Total a Pagar".
    Texto: {texto[:4000]}
    """
    for _ in range(2):
        try:
            resp = model.generate_content(prompt)
            clean = resp.text.replace('```json', '').replace('```', '').strip()
            d = json.loads(clean)
            return {
                'valor': float(d.get('valor') or 0),
                'codigo': limpar_digitos(d.get('codigo')),
                'empresa': d.get('empresa') or ""
            }
        except: time.sleep(0.5)
    return {'valor': 0.0, 'codigo': '', 'empresa': ''}

def extrair_forca_bruta(texto):
    """
    Tenta salvar boletos que a IA falhou (especialmente PMSP).
    """
    dados = {'valor': 0.0, 'codigo': ''}
    
    # 1. Código
    t_limpo = texto.replace('\n','').replace(' ','').replace('.','').replace('-','')
    match_cod = re.search(r'\d{44,48}', t_limpo)
    if match_cod: dados['codigo'] = match_cod.group(0)
    
    # 2. Valor (Procura formato monetário brasileiro)
    # Pega todos os valores possiveis
    valores = re.findall(r'(?:R\$\s?)?(\d{1,3}(?:\.?\d{3})*,\d{2})', texto)
    floats = []
    for v in valores:
        try: floats.append(float(v.replace('.','').replace(',','.')))
        except: pass
    
    if floats:
        # Heurística: O valor do boleto geralmente é o maior valor numérico na página
        # (para fugir de juros, multas, ou valores parciais menores)
        dados['valor'] = max(floats)
        
    return dados

# --- LÓGICA DO FUNIL ---

def processar_conciliacao_json_stream(lista_caminhos_boletos, caminho_comprovantes, user):
    
    def send(type, data):
        return json.dumps({'type': type, 'data': data}) + "\n"

    # Atualiza lista visual
    yield send('init_list', {'files': [os.path.basename(p) for p in lista_caminhos_boletos]})
    yield send('log', '🚀 Iniciando Estratégia de Funil (Eliminação)...')

    # Listas Globais
    todos_boletos: List[Documento] = []
    todos_comprovantes: List[Documento] = []
    total_paginas = 0

    # ==========================================
    # FASE 1: LEITURA COMPLETA (CARREGAR TUDO)
    # ==========================================
    yield send('log', '📥 Lendo TODOS os Comprovantes...')
    
    reader_comp = PdfReader(caminho_comprovantes)
    total_paginas += len(reader_comp.pages)
    
    for i, page in enumerate(reader_comp.pages):
        texto = page.extract_text() or ""
        
        # Extração
        dados = extrair_dados_ia(texto, "comprovante bancario")
        
        # Cria Objeto
        writer = PdfWriter()
        writer.add_page(page)
        bio = io.BytesIO()
        writer.write(bio)
        
        doc = Documento(
            id=f"COMP-{i}",
            nome_arquivo=f"Página {i+1}",
            texto=texto,
            pdf_object=bio,
            valor=dados['valor'],
            codigo=dados['codigo'],
            empresa=dados['empresa']
        )
        todos_comprovantes.append(doc)
        
        emp_curta = (doc.empresa or "?")[:10]
        yield send('comp_status', {'index': i, 'msg': f'R${doc.valor} ({emp_curta})'})

    yield send('log', '📥 Lendo TODOS os Boletos...')
    
    for path in lista_caminhos_boletos:
        nome = os.path.basename(path)
        yield send('file_start', {'filename': nome})
        
        reader = PdfReader(path)
        texto = ""
        for p in reader.pages: texto += p.extract_text() or ""
        total_paginas += len(reader.pages)
        
        # Tenta Regex primeiro (rápido)
        dados = extrair_forca_bruta(texto)
        if dados['valor'] == 0:
            # Se regex falhou, IA neles
            yield send('log', f'   > IA analisando {nome}...')
            dados_ia = extrair_dados_ia(texto, "boleto/guia")
            # Merge: o que a IA achou substitui
            if dados_ia['valor'] > 0: dados['valor'] = dados_ia['valor']
            if dados_ia['codigo']: dados['codigo'] = dados_ia['codigo']
            dados['empresa'] = dados_ia.get('empresa', '')

        # Binário
        with open(path, 'rb') as f:
            bio = io.BytesIO(f.read())

        doc = Documento(
            id=f"BOL-{nome}",
            nome_arquivo=nome,
            texto=texto,
            pdf_object=bio,
            valor=dados['valor'],
            codigo=dados['codigo'],
            empresa=dados.get('empresa', '')
        )
        todos_boletos.append(doc)
        
        # Correção visual na lista
        status_icon = 'warning' if doc.valor == 0 else 'processing'
        yield send('file_done', {'filename': nome, 'status': status_icon})

    # ==========================================
    # FASE 2: O FUNIL DE ELIMINAÇÃO
    # ==========================================
    yield send('log', '🌪️ Iniciando Cruzamento de Dados...')
    
    # Rodada 1: CÓDIGO DE BARRAS (Alta Confiança)
    yield send('log', '   > Passada 1: Código de Barras...')
    count_r1 = 0
    for bol in todos_boletos:
        if bol.conciliado_com: continue
        for comp in todos_comprovantes:
            if comp.conciliado_com: continue
            
            if bol.codigo and comp.codigo:
                # Match exato ou match dos primeiros 20 digitos
                if bol.codigo == comp.codigo or \
                   (len(bol.codigo)>20 and bol.codigo.startswith(comp.codigo[:20])) or \
                   (len(comp.codigo)>20 and comp.codigo.startswith(bol.codigo[:20])):
                    
                    bol.conciliado_com = comp
                    comp.conciliado_com = bol
                    bol.metodo_match = "CÓDIGO"
                    count_r1 += 1
                    break
    yield send('log', f'     {count_r1} conciliados.')

    # Rodada 2: VALOR + EMPRESA (Média Confiança)
    yield send('log', '   > Passada 2: Valor + Empresa...')
    count_r2 = 0
    for bol in todos_boletos:
        if bol.conciliado_com: continue
        if bol.valor == 0: continue
        
        for comp in todos_comprovantes:
            if comp.conciliado_com: continue
            
            # Tolerância de 5 centavos
            if abs(bol.valor - comp.valor) < 0.05:
                # Checa nomes
                n1 = normalizar_empresa(bol.empresa)
                n2 = normalizar_empresa(comp.empresa)
                
                match_nome = False
                if n1 == "GOVERNO" and n2 == "GOVERNO": match_nome = True
                elif n1 and n2 and (n1 in n2 or n2 in n1): match_nome = True
                elif n1 and n2 and n1.split()[0] == n2.split()[0]: match_nome = True
                
                if match_nome:
                    bol.conciliado_com = comp
                    comp.conciliado_com = bol
                    bol.metodo_match = f"VALOR+NOME ({n1})"
                    count_r2 += 1
                    break
    yield send('log', f'     {count_r2} conciliados.')

    # Rodada 3: VALOR EXATO (Baixa Confiança - "O que sobrou")
    yield send('log', '   > Passada 3: Apenas Valor (Sobra)...')
    count_r3 = 0
    for bol in todos_boletos:
        if bol.conciliado_com: continue
        if bol.valor == 0: continue
        
        candidatos = [c for c in todos_comprovantes if not c.conciliado_com and abs(c.valor - bol.valor) < 0.01]
        
        # Se houver APENAS UM comprovante sobrando com esse valor exato, é match
        if len(candidatos) == 1:
            comp = candidatos[0]
            bol.conciliado_com = comp
            comp.conciliado_com = bol
            bol.metodo_match = "VALOR (Único)"
            count_r3 += 1
            
    yield send('log', f'     {count_r3} conciliados.')

    # Rodada 4: A REPESCAGEM FINAL (Desespero)
    # Se sobrou 1 boleto e 1 comprovante, assume que são par
    sobras_bol = [b for b in todos_boletos if not b.conciliado_com]
    sobras_comp = [c for c in todos_comprovantes if not c.conciliado_com]
    
    if len(sobras_bol) == 1 and len(sobras_comp) == 1:
        sb = sobras_bol[0]
        sc = sobras_comp[0]
        # Só casa se os valores não forem absurdamente diferentes (tipo 10 reais vs 1000 reais)
        # Se for 402.00 vs 0.00 (erro de leitura), a gente casa
        sb.conciliado_com = sc
        sc.conciliado_com = sb
        sb.metodo_match = "REPESCAGEM (Últimos)"
        yield send('log', '   > Passada 4: Repescagem final (1x1).')

    # ==========================================
    # FASE 3: GERAR SAÍDA
    # ==========================================
    yield send('log', '💾 Gerando arquivos finais...')
    
    output_zip_buffer = io.BytesIO()
    with zipfile.ZipFile(output_zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for bol in todos_boletos:
            writer_final = PdfWriter()
            
            # Boleto Original
            bol.pdf_object.seek(0)
            reader_b = PdfReader(bol.pdf_object)
            for p in reader_b.pages: writer_final.add_page(p)
            
            status = 'warning'
            msg = 'Não encontrado'
            
            if bol.conciliado_com:
                status = 'success'
                msg = bol.metodo_match
                # Comprovante
                bol.conciliado_com.pdf_object.seek(0)
                reader_c = PdfReader(bol.conciliado_com.pdf_object)
                writer_final.add_page(reader_c.pages[0])
            
            # Atualiza UI final
            yield send('file_done', {'filename': bol.nome_arquivo, 'status': status})
            yield send('log', f"{'✅' if status=='success' else '❌'} {bol.nome_arquivo} -> {msg}")
            
            # Salva no ZIP
            bio = io.BytesIO()
            writer_final.write(bio)
            zip_file.writestr(bol.nome_arquivo, bio.getvalue())

    # Finaliza
    pasta = os.path.join(settings.MEDIA_ROOT, 'downloads')
    os.makedirs(pasta, exist_ok=True)
    nome_zip = f"Conciliacao_Funil_{uuid.uuid4().hex[:8]}.zip"
    
    with open(os.path.join(pasta, nome_zip), "wb") as f:
        f.write(output_zip_buffer.getvalue())

    if hasattr(user, 'paginas_processadas'):
        user.paginas_processadas += total_paginas
        user.save()

    yield send('finish', {'url': f"{settings.MEDIA_URL}downloads/{nome_zip}", 'total': total_paginas})