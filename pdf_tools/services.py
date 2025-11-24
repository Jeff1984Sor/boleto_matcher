import io
import os
import zipfile
import time
import uuid
import json
import re
from pypdf import PdfReader, PdfWriter
from django.conf import settings

# --- FERRAMENTAS DE LIMPEZA ---

def limpar_tudo_deixar_numeros(texto):
    """
    Transforma '816-2.00' em '816200'.
    Adiciona 'A' na frente internamente só para garantir que
    o sistema trate como Texto e não perca zeros à esquerda.
    """
    if not texto: return ""
    numeros = re.sub(r'\D', '', str(texto))
    if not numeros: return ""
    return numeros # Retorna string pura de números

def extrair_valor_do_nome(nome_arquivo):
    """
    Lê o valor escrito no nome do arquivo.
    Ex: 'Boleto - R$ 402_00.pdf' -> 402.00
    """
    match = re.search(r'R\$\s?(\d+)[_.,-](\d{2})', nome_arquivo)
    if match:
        try:
            return float(f"{match.group(1)}.{match.group(2)}")
        except: pass
    return 0.0

def extrair_dados_brutos(texto, nome_arquivo=""):
    """
    Extrai Código (Longo) e Valor (Maior da página).
    """
    dados = {'codigo': '', 'valor': 0.0}
    
    # 1. CÓDIGO: Procura sequência de pelo menos 36 dígitos
    # Remove quebras de linha e espaços para juntar o código
    texto_limpo = texto.replace('\n', '').replace(' ', '').replace('.', '').replace('-', '')
    match_cod = re.search(r'\d{36,}', texto_limpo)
    if match_cod:
        dados['codigo'] = match_cod.group(0)
        
    # 2. VALOR: Procura formato monetário
    valores = re.findall(r'(?:R\$\s?)?(\d{1,3}(?:\.?\d{3})*,\d{2})', texto)
    floats = []
    for v in valores:
        try: floats.append(float(v.replace('.', '').replace(',', '.')))
        except: pass
    
    if floats:
        dados['valor'] = max(floats) # Assume o maior valor (Total)
        
    # 3. RECUPERAÇÃO: Se valor for 0, tenta ler do nome do arquivo
    if dados['valor'] == 0 and nome_arquivo:
        val_nome = extrair_valor_do_nome(nome_arquivo)
        if val_nome > 0:
            dados['valor'] = val_nome
            
    return dados

# --- MOTOR PRINCIPAL ---

def processar_conciliacao_json_stream(lista_caminhos_boletos, caminho_comprovantes, user):
    
    def send(type, data):
        return json.dumps({'type': type, 'data': data}) + "\n"

    yield send('init_list', {'files': [os.path.basename(p) for p in lista_caminhos_boletos]})
    yield send('log', '🚀 Iniciando Conciliação Exata (String Pura)...')

    # Tabela Virtual de Comprovantes (Memória)
    tabela_comprovantes = []
    
    # =======================================================
    # 1. POPULAR TABELA (LER COMPROVANTES)
    # =======================================================
    yield send('log', '📂 Indexando Comprovantes...')
    reader_comp = PdfReader(caminho_comprovantes)
    
    for i, page in enumerate(reader_comp.pages):
        texto = page.extract_text() or ""
        dados = extrair_dados_brutos(texto)
        
        writer = PdfWriter()
        writer.add_page(page)
        bio = io.BytesIO()
        writer.write(bio)
        
        item = {
            'id': i,
            'origem': f"Pag {i+1}",
            'codigo': limpar_tudo_deixar_numeros(dados['codigo']),
            'valor': dados['valor'],
            'pdf_bytes': bio,
            'usado': False
        }
        tabela_comprovantes.append(item)
        
        # Mostra começo e fim do código para validação
        cod_log = "Sem Código"
        if item['codigo']:
            c = item['codigo']
            cod_log = f"{c[:6]}...{c[-6:]}"
            
        yield send('comp_status', {'index': i, 'msg': f"R${item['valor']} | {cod_log}"})

    # =======================================================
    # 2. LER BOLETOS E CONCILIAR (LOOP SEQUENCIAL)
    # =======================================================
    yield send('log', '⚡ Conciliando Boletos...')
    
    lista_boletos_final = []
    
    for path in lista_caminhos_boletos:
        nome_arq = os.path.basename(path)
        yield send('file_start', {'filename': nome_arq})
        
        reader = PdfReader(path)
        texto = ""
        for p in reader.pages: texto += p.extract_text() or ""
        
        # Extração
        dados = extrair_dados_brutos(texto, nome_arq)
        
        if dados['valor'] == 0:
             yield send('log', f"⚠️ Valor 0.0 recuperado do nome: {nome_arq}")

        with open(path, 'rb') as f: bio = io.BytesIO(f.read())
        
        boleto = {
            'nome': nome_arq,
            'codigo': limpar_tudo_deixar_numeros(dados['codigo']),
            'valor': dados['valor'],
            'pdf_bytes': bio,
            'match': None,
            'tipo_match': ''
        }

        # --- MATCHING ---
        encontrado = False
        
        # TENTATIVA 1: CÓDIGO EXATO (String Completa)
        # Verifica se a string do boleto é IGUAL ou ESTÁ CONTIDA na do comprovante (ou vice versa)
        if boleto['codigo']:
            for comp in tabela_comprovantes:
                if comp['usado']: continue
                
                if comp['codigo'] and (boleto['codigo'] in comp['codigo'] or comp['codigo'] in boleto['codigo']):
                    encontrado = True
                    comp['usado'] = True
                    boleto['match'] = comp
                    boleto['tipo_match'] = "CÓDIGO EXATO"
                    break
        
        # TENTATIVA 2: VALOR (Fila Sequencial)
        # Se o código falhou (por ser diferente ou não existir), usa o valor
        if not encontrado and boleto['valor'] > 0:
            for comp in tabela_comprovantes:
                if comp['usado']: continue
                
                # Margem minima de erro float
                if abs(boleto['valor'] - comp['valor']) < 0.05:
                    encontrado = True
                    comp['usado'] = True
                    boleto['match'] = comp
                    boleto['tipo_match'] = "VALOR (Fila)"
                    break
        
        # Feedback
        status_ui = 'warning'
        if encontrado:
            status_ui = 'success'
            yield send('log', f"✅ {nome_arq} -> {boleto['tipo_match']}")
        else:
            yield send('log', f"❌ {nome_arq} (R${boleto['valor']}) -> Sem par.")

        yield send('file_done', {'filename': nome_arq, 'status': status_ui})
        lista_boletos_final.append(boleto)

    # =======================================================
    # 3. GERAR ARQUIVO
    # =======================================================
    yield send('log', '💾 Salvando...')
    
    output_zip_buffer = io.BytesIO()
    with zipfile.ZipFile(output_zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for item in lista_boletos_final:
            writer_final = PdfWriter()
            
            # Paginas do Boleto
            item['pdf_bytes'].seek(0)
            rb = PdfReader(item['pdf_bytes'])
            for p in rb.pages: writer_final.add_page(p)
            
            # Pagina do Comprovante
            if item['match']:
                item['match']['pdf_bytes'].seek(0)
                rc = PdfReader(item['match']['pdf_bytes'])
                writer_final.add_page(rc.pages[0])
            
            bio = io.BytesIO()
            writer_final.write(bio)
            zip_file.writestr(item['nome'], bio.getvalue())

    # Finaliza
    pasta = os.path.join(settings.MEDIA_ROOT, 'downloads')
    os.makedirs(pasta, exist_ok=True)
    nome_zip = f"Conciliacao_Exata_{uuid.uuid4().hex[:8]}.zip"
    
    with open(os.path.join(pasta, nome_zip), "wb") as f:
        f.write(output_zip_buffer.getvalue())
        
    total = len(tabela_comprovantes) + len(lista_boletos_final)
    if hasattr(user, 'paginas_processadas'):
        user.paginas_processadas += total
        user.save()

    yield send('finish', {'url': f"{settings.MEDIA_URL}downloads/{nome_zip}", 'total': total})