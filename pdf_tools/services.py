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

# Configura IA apenas para extrair Data e Empresa (nomes difíceis)
genai.configure(api_key=settings.GOOGLE_API_KEY)

# --- FERRAMENTAS DE LIMPEZA (DO JEITO QUE VC PEDIU) ---

def limpar_string_numerica(texto):
    """
    RECEBE: "8169.0000.0042  020000..."
    RETORNA: "816900000042020000..."
    Remove tudo que não for número.
    """
    if not texto: return ""
    # Regex \D pega tudo que NÃO é dígito e substitui por vazio
    limpo = re.sub(r'\D', '', str(texto))
    return limpo

def formatar_moeda(valor_float):
    """Converte float 402.0 em string '402,00' para logs."""
    return f"{valor_float:,.2f}".replace('.', '#').replace(',', '.').replace('#', ',')

def extrair_dados_brutos(texto):
    """
    Extrai Código e Valor usando REGEX PURO (Sem IA).
    """
    dados = {'codigo': '', 'valor': 0.0}
    
    # 1. CÓDIGO DE BARRAS / LINHA DIGITÁVEL
    # Procura sequencias longas de numeros (com ou sem pontos/espaços no meio)
    # Pega grupos de numeros que tenham pelo menos 30 digitos no total
    match_cod = re.search(r'(?:\d[\.\s\-\_]*){36,}', texto)
    if match_cod:
        # Limpa imediatamente
        dados['codigo'] = limpar_string_numerica(match_cod.group(0))
        
    # 2. VALOR (Formato Brasileiro ou Americano)
    # Busca por padrões como 1.000,00 ou 402,00
    # A regex pega o último valor numérico monetário da página (geralmente é o Total)
    valores = re.findall(r'(?:R\$\s?)?(\d{1,3}(?:\.?\d{3})*,\d{2})', texto)
    
    if valores:
        floats = []
        for v in valores:
            try:
                # Converte 1.000,00 para 1000.00 (float python)
                v_clean = v.replace('.', '').replace(',', '.')
                floats.append(float(v_clean))
            except: pass
        
        if floats:
            # Pega o maior valor encontrado (evita pegar valor de multa ou juros menores)
            dados['valor'] = max(floats)
            
    return dados

def match_codigos(cod_a, cod_b):
    """
    Compara dois códigos numéricos limpos.
    Retorna True se forem compatíveis.
    """
    if not cod_a or not cod_b: return False
    
    # Match Exato
    if cod_a == cod_b: return True
    
    # Match de Contenção (Um dentro do outro)
    # Ex: Boleto tem digito verificador a mais que o comprovante cortou
    if cod_a in cod_b or cod_b in cod_a: return True
    
    # Match de "Miolo" (Para resolver o seu caso da imagem)
    # As imagens mostram que o começo e o fim mudam, mas o MEIO é igual.
    # Vamos verificar se eles compartilham uma sequencia de 24 numeros iguais.
    
    # Pega pedaços de 24 digitos do código A e vê se existe no B
    tamanho_janela = 24
    if len(cod_a) < tamanho_janela: return False
    
    for i in range(len(cod_a) - tamanho_janela + 1):
        janela = cod_a[i : i + tamanho_janela]
        if janela in cod_b:
            return True
            
    return False

# --- PROCESSO PRINCIPAL ---

def processar_conciliacao_json_stream(lista_caminhos_boletos, caminho_comprovantes, user):
    
    def send(type, data):
        return json.dumps({'type': type, 'data': data}) + "\n"

    yield send('init_list', {'files': [os.path.basename(p) for p in lista_caminhos_boletos]})
    yield send('log', '🚀 Iniciando Sistema (Modo Regex Puro)...')

    # Estrutura da Tabela Virtual
    # Cada item: {'id': 0, 'codigo': '816...', 'valor': 402.0, 'usado': False, 'pdf': binary}
    tabela_comprovantes = []
    
    # =======================================================
    # 1. LER COMPROVANTES (Montar a Tabela)
    # =======================================================
    yield send('log', '📂 Lendo Comprovantes...')
    
    reader_comp = PdfReader(caminho_comprovantes)
    total_paginas = len(reader_comp.pages)
    
    for i, page in enumerate(reader_comp.pages):
        texto = page.extract_text() or ""
        
        # Extrai na força bruta (Regex)
        dados = extrair_dados_brutos(texto)
        
        # Salva o PDF desta página na memória
        writer = PdfWriter()
        writer.add_page(page)
        bio = io.BytesIO()
        writer.write(bio)
        
        item = {
            'id': i,
            'origem': f"Página {i+1}",
            'codigo': dados['codigo'], # Já vem limpo (string apenas numeros)
            'valor': dados['valor'],
            'pdf_bytes': bio,
            'usado': False
        }
        tabela_comprovantes.append(item)
        
        # Log para você conferir se limpou certo
        cod_log = f"A{item['codigo'][:15]}..." if item['codigo'] else "Sem Código"
        val_log = formatar_moeda(item['valor'])
        yield send('comp_status', {'index': i, 'msg': f"{val_log} | {cod_log}"})

    # =======================================================
    # 2. LER BOLETOS E COMPARAR
    # =======================================================
    yield send('log', '⚡ Lendo Boletos e Comparando...')
    
    # Lista para salvar os resultados e gerar o zip depois
    resultados = [] # [{'nome': 'x.pdf', 'status': 'ok', 'boleto_pdf': b, 'comp_pdf': b}]
    
    for path in lista_caminhos_boletos:
        nome_arq = os.path.basename(path)
        yield send('file_start', {'filename': nome_arq})
        
        reader = PdfReader(path)
        texto = ""
        for p in reader.pages: texto += p.extract_text() or ""
        total_paginas += len(reader.pages)
        
        # Extrai Boleto
        dados_bol = extrair_dados_brutos(texto)
        
        # Se Regex falhou no valor (caso PMSP R$ 0.0), tenta pegar do Nome do Arquivo
        # Ex: "Boleto - R$ 402_00.pdf"
        if dados_bol['valor'] == 0:
            match_nome = re.search(r'R\$\s?(\d+)[_,.](\d{2})', nome_arq)
            if match_nome:
                dados_bol['valor'] = float(f"{match_nome.group(1)}.{match_nome.group(2)}")
        
        with open(path, 'rb') as f: bio_bol = io.BytesIO(f.read())
        
        boleto = {
            'nome': nome_arq,
            'codigo': dados_bol['codigo'],
            'valor': dados_bol['valor'],
            'pdf_bytes': bio_bol,
            'match': None
        }
        
        # --- LÓGICA DE MATCH (SEQUENCIAL) ---
        match_encontrado = False
        
        # TENTATIVA 1: PELO CÓDIGO LIMPO (String match)
        if boleto['codigo']:
            for comp in tabela_comprovantes:
                if comp['usado']: continue # Se já usou, pula
                
                # Compara as strings limpas
                if match_codigos(boleto['codigo'], comp['codigo']):
                    boleto['match'] = comp
                    comp['usado'] = True
                    match_encontrado = True
                    yield send('log', f"✅ Match Código: {nome_arq}")
                    break
        
        # TENTATIVA 2: PELO VALOR (Se falhar código)
        if not match_encontrado and boleto['valor'] > 0:
            for comp in tabela_comprovantes:
                if comp['usado']: continue
                
                # Compara float com margem pequena (0.05 centavos)
                if abs(boleto['valor'] - comp['valor']) < 0.05:
                    boleto['match'] = comp
                    comp['usado'] = True
                    match_encontrado = True
                    val_fmt = formatar_moeda(boleto['valor'])
                    yield send('log', f"⚠️ Match Valor: {nome_arq} ({val_fmt})")
                    break

        # Atualiza Status Visual
        status = 'success' if match_encontrado else 'warning'
        yield send('file_done', {'filename': nome_arq, 'status': status})
        
        resultados.append(boleto)

    # =======================================================
    # 3. REPESCAGEM FINAL (VOLTA NOS QUE SOBRARAM)
    # =======================================================
    yield send('log', '🔄 Repescagem nos sobras...')
    
    for boleto in resultados:
        if boleto['match']: continue # Já tem par
        
        # Tenta achar qualquer comprovante livre com o mesmo valor
        if boleto['valor'] > 0:
            for comp in tabela_comprovantes:
                if comp['usado']: continue
                
                if abs(boleto['valor'] - comp['valor']) < 0.05:
                    boleto['match'] = comp
                    comp['usado'] = True
                    yield send('log', f"🔗 Match Repescagem: {boleto['nome']}")
                    yield send('file_done', {'filename': boleto['nome'], 'status': 'success'})
                    break
    
    # =======================================================
    # 4. GERAR ARQUIVO FINAL
    # =======================================================
    yield send('log', '💾 Gerando PDF Combinado...')
    
    output_zip_buffer = io.BytesIO()
    with zipfile.ZipFile(output_zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for item in resultados:
            writer_final = PdfWriter()
            
            # Adiciona Boleto
            item['pdf_bytes'].seek(0)
            rb = PdfReader(item['pdf_bytes'])
            for p in rb.pages: writer_final.add_page(p)
            
            # Adiciona Comprovante (se achou)
            if item['match']:
                item['match']['pdf_bytes'].seek(0)
                rc = PdfReader(item['match']['pdf_bytes'])
                writer_final.add_page(rc.pages[0])
            
            # Salva
            bio = io.BytesIO()
            writer_final.write(bio)
            zip_file.writestr(item['nome'], bio.getvalue())

    # Finaliza
    pasta = os.path.join(settings.MEDIA_ROOT, 'downloads')
    os.makedirs(pasta, exist_ok=True)
    nome_zip = f"Conciliacao_Limpa_{uuid.uuid4().hex[:8]}.zip"
    
    with open(os.path.join(pasta, nome_zip), "wb") as f:
        f.write(output_zip_buffer.getvalue())

    if hasattr(user, 'paginas_processadas'):
        user.paginas_processadas += total_paginas
        user.save()

    yield send('finish', {'url': f"{settings.MEDIA_URL}downloads/{nome_zip}", 'total': total_paginas})