import io
import os
import zipfile
import uuid
import json
import re
import logging
import time
import concurrent.futures
import fitz  # PyMuPDF
from pypdf import PdfReader, PdfWriter
from PIL import Image
import google.generativeai as genai
from django.conf import settings

# ConfiguraÃƒÂ§ÃƒÂ£o do logger
logger = logging.getLogger(__name__)

# Configura a API do Google Gemini
genai.configure(api_key=settings.GOOGLE_API_KEY)

# ============================================================
# FERRAMENTAS AUXILIARES
# ============================================================

def gerar_conteudo_com_timeout(model, parts, timeout_s):
    """Executa generate_content com timeout para evitar travar o stream."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(model.generate_content, parts)
        try:
            return future.result(timeout=timeout_s)
        except concurrent.futures.TimeoutError:
            logger.error(f"Timeout na chamada do Gemini ({timeout_s}s).")
            return None

def limpar_numeros(texto):
    """Remove todos os caracteres nÃƒÂ£o numÃƒÂ©ricos de uma string."""
    return re.sub(r'\D', '', str(texto or ""))

def linha_digitavel_bancaria_para_codigo(linha):
    """Converte linha digitavel bancaria (47) em codigo de barras (44)."""
    if not linha or len(linha) != 47 or not linha.isdigit():
        return ""
    return f"{linha[0:4]}{linha[32]}{linha[33:37]}{linha[37:47]}{linha[4:9]}{linha[10:20]}{linha[21:31]}"

def linha_digitavel_arrecadacao_para_codigo(linha):
    """Converte linha digitavel de arrecadacao (48) em codigo de barras (44)."""
    if not linha or len(linha) != 48 or not linha.isdigit():
        return ""
    return f"{linha[0:11]}{linha[12:23]}{linha[24:35]}{linha[36:47]}"

def normalizar_codigo_barras(codigo):
    """
    Normaliza o codigo para formato comparavel.
    Remove pontos/tracos e converte linha digitavel para codigo de barras.
    """
    bruto = str(codigo or "")
    somente_numeros = limpar_numeros(bruto)
    if not somente_numeros:
        return ""

    candidatos = [somente_numeros]
    if len(somente_numeros) not in (44, 47, 48):
        for match in re.finditer(r'(?:\d[\s.\-]*){44,48}', bruto):
            c = limpar_numeros(match.group(0))
            if len(c) in (44, 47, 48):
                candidatos.append(c)

    for cand in candidatos:
        if len(cand) == 44:
            return cand
        if len(cand) == 47:
            convertido = linha_digitavel_bancaria_para_codigo(cand)
            if convertido:
                return convertido
        if len(cand) == 48:
            convertido = linha_digitavel_arrecadacao_para_codigo(cand)
            if convertido:
                return convertido
    return somente_numeros

def codigos_sao_iguais(codigo_a, codigo_b):
    """Compara codigos apos normalizacao."""
    a = normalizar_codigo_barras(codigo_a)
    b = normalizar_codigo_barras(codigo_b)
    return bool(a and b and a == b)

def normalizar_valor(v_str):
    """Converte uma string de valor monetÃƒÂ¡rio para float."""
    try:
        if isinstance(v_str, (float, int)): return float(v_str)
        v = str(v_str).replace('R$', '').strip()
        if ',' in v and '.' in v: v = v.replace('.', '').replace(',', '.')
        elif ',' in v: v = v.replace(',', '.')
        return float(v)
    except (ValueError, TypeError):
        return 0.0

def extrair_valor_nome(nome_arquivo):
    """Tenta extrair um valor monetÃƒÂ¡rio do nome do arquivo."""
    match = re.search(r'R\$\s?(\d+)[_.,-](\d{2})', nome_arquivo)
    if match:
        try:
            return float(f"{match.group(1)}.{match.group(2)}")
        except: pass
    return 0.0

def normalizar_texto(texto):
    return re.sub(r'\s+', ' ', str(texto or '').strip()).upper()

def cnpj_sao_iguais(cnpj_a, cnpj_b):
    a = limpar_numeros(cnpj_a)
    b = limpar_numeros(cnpj_b)
    return bool(a and b and a == b)

def nomes_parecidos(nome_a, nome_b):
    a = normalizar_texto(nome_a)
    b = normalizar_texto(nome_b)
    if not a or not b:
        return False
    return a == b or a in b or b in a

def extrair_referencia_nome_arquivo(nome_arquivo):
    partes = str(nome_arquivo or '').split(' - ')
    if len(partes) < 2:
        return ''
    referencia = re.sub(r'\(.*?\)', '', partes[1]).strip()
    return normalizar_texto(referencia)

def referencia_aparece_no_texto(referencia, texto):
    ref = normalizar_texto(referencia)
    txt = normalizar_texto(texto)
    return bool(ref and txt and ref in txt)

def datas_sao_iguais(data_a, data_b):
    return bool(data_a and data_b and str(data_a) == str(data_b))

def valores_sao_iguais(valor_a, valor_b, tolerancia=0.05):
    try:
        return float(valor_a or 0) > 0 and float(valor_b or 0) > 0 and abs(float(valor_a) - float(valor_b)) < tolerancia
    except (TypeError, ValueError):
        return False

def diferenca_valor(valor_a, valor_b):
    try:
        return abs(float(valor_a or 0) - float(valor_b or 0))
    except (TypeError, ValueError):
        return float('inf')

def calcular_score_match(boleto, comprovante):
    bd = boleto.get('dados_completos', {})
    cd = comprovante.get('dados_completos', {})
    score = 0
    motivos = []

    if boleto.get('codigo') and comprovante.get('codigo') and codigos_sao_iguais(boleto.get('codigo'), comprovante.get('codigo')):
        score += 60
        motivos.append('codigo_barras')

    if cnpj_sao_iguais(bd.get('cnpj_pagador'), cd.get('cnpj_pagador')):
        score += 20
        motivos.append('cnpj_pagador')
    if cnpj_sao_iguais(bd.get('cnpj_beneficiario'), cd.get('cnpj_beneficiario')):
        score += 20
        motivos.append('cnpj_beneficiario')

    if nomes_parecidos(bd.get('nome_pagador'), cd.get('nome_pagador')):
        score += 10
        motivos.append('nome_pagador')
    if nomes_parecidos(bd.get('nome_beneficiario'), cd.get('nome_beneficiario')):
        score += 10
        motivos.append('nome_beneficiario')

    if boleto.get('valor', 0) > 0 and comprovante.get('valor', 0) > 0:
        diferenca = abs(boleto['valor'] - comprovante['valor'])
        if diferenca < 0.01:
            score += 30
            motivos.append('valor_exato')
        elif diferenca < 0.05:
            score += 20
            motivos.append('valor_tolerancia')

    data_boleto = bd.get('data_pagamento') or bd.get('data_vencimento')
    data_comp = cd.get('data_pagamento') or cd.get('data_vencimento')
    if datas_sao_iguais(data_boleto, data_comp):
        score += 8
        motivos.append('data')

    return score, motivos

def serializar_extracao_item(item, tipo):
    d = item.get('dados_completos', {})
    base = {
        'tipo': tipo,
        'valor': item.get('valor'),
        'codigo_barras': item.get('codigo', ''),
        'nome_pagador': d.get('nome_pagador'),
        'cnpj_pagador': d.get('cnpj_pagador'),
        'nome_beneficiario': d.get('nome_beneficiario'),
        'cnpj_beneficiario': d.get('cnpj_beneficiario'),
        'data_pagamento': d.get('data_pagamento'),
        'data_vencimento': d.get('data_vencimento'),
        'dados_completos': d,
    }
    if tipo == 'comprovante':
        base['pagina'] = item.get('id', 0) + 1
    else:
        base['arquivo'] = item.get('nome')
    return base

def pdf_bytes_para_imagem_pil(pdf_bytes):
    """Converte a primeira pÃƒÂ¡gina de um PDF em uma imagem PIL de alta qualidade."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    page = doc[0]
    matriz_zoom = fitz.Matrix(2, 2)
    pix = page.get_pixmap(matrix=matriz_zoom)
    return Image.open(io.BytesIO(pix.tobytes("jpeg")))

# ============================================================
# NOVA FUNÃƒâ€¡ÃƒÆ’O DE EXTRAÃƒâ€¡ÃƒÆ’O ESTRUTURADA COM IA
# ============================================================

def extrair_dados_estruturados_com_ia(imagem_pil, tipo_doc):
    """
    Usa um modelo de IA para extrair um JSON estruturado de uma imagem de documento.
    """
    model = genai.GenerativeModel(settings.GEMINI_MODEL)
    timeout_s = int(os.getenv('GEMINI_TIMEOUT_SECONDS', '300'))
    prompt = f"""
    Analise esta imagem de um {tipo_doc}.
    Extraia os dados com foco em conciliacao financeira em cenario de muitos documentos com o MESMO valor.
    Retorne APENAS um objeto JSON valido.

    Campos obrigatorios no JSON:
    1. `codigo_barras_numerico`: linha digitavel ou codigo de barras com APENAS numeros.
    2. `data_vencimento`: data de vencimento (YYYY-MM-DD) ou null.
    3. `data_pagamento`: data de pagamento (YYYY-MM-DD) ou null.
    4. `valor_float`: valor principal como numero (ex: 123.45).
    5. `valor_virgula`: mesmo valor em string com virgula (ex: "123,45").
    6. `nome_beneficiario`: quem recebe o pagamento.
    7. `nome_pagador`: quem paga.
    8. `cnpj_beneficiario`: CNPJ do beneficiario.
    9. `cnpj_pagador`: CNPJ do pagador.

    Campos auxiliares de desempate (tambem retornar, se existirem):
    - `banco_emissor`
    - `agencia_codigo_beneficiario`
    - `nosso_numero`
    - `numero_documento`
    - `autenticacao_mecanica`

    REGRAS IMPORTANTES:
    - Se um campo nao for encontrado, use `null`.
    - Nao invente dados; preencha apenas o que estiver legivel no documento.
    - Nao troque pagador e beneficiario.
    - Em documentos com mesmo valor, priorize identificadores fortes (linha digitavel/codigo de barras, CNPJ, nosso numero, numero do documento).
    - O JSON de saida nao pode conter markdown nem blocos ```json.

    Exemplo de saida:
    {{
      "codigo_barras_numerico": "34191790010352013781368109400000187220000015000",
      "data_vencimento": "2024-07-31",
      "data_pagamento": "2024-07-30",
      "valor_float": 150.00,
      "valor_virgula": "150,00",
      "nome_beneficiario": "MINHA EMPRESA LTDA",
      "nome_pagador": "CLIENTE EXEMPLO SA",
      "cnpj_beneficiario": "12.345.678/0001-99",
      "cnpj_pagador": "98.765.432/0001-11",
      "banco_emissor": "341",
      "agencia_codigo_beneficiario": "1234/567890",
      "nosso_numero": "109400000187220",
      "numero_documento": "FAT-9981",
      "autenticacao_mecanica": null
    }}
    """
    for tentativa in range(3):
        try:
            response = gerar_conteudo_com_timeout(model, [prompt, imagem_pil], timeout_s)
            if response is None:
                time.sleep(2 * (tentativa + 1))
                continue
            texto_resposta = response.text.strip()
            if texto_resposta.startswith("```json"):
                texto_resposta = texto_resposta[7:]
            if texto_resposta.endswith("```"):
                texto_resposta = texto_resposta[:-3]
            return json.loads(texto_resposta.strip())
        except (json.JSONDecodeError, Exception) as e:
            logger.error(f"Erro na extraÃƒÂ§ÃƒÂ£o estruturada (tentativa {tentativa+1}): {e}")
            time.sleep(2 * (tentativa + 1))
    return {}

# ============================================================
# FUNÃƒâ€¡Ãƒâ€¢ES DO FLUXO PRINCIPAL (ATUALIZADAS)
# ============================================================

def processar_pagina(pdf_bytes, tipo_doc, nome_arquivo=""):
    """
    Processa uma pÃƒÂ¡gina de PDF, usando a extraÃƒÂ§ÃƒÂ£o estruturada.
    """
    try:
        imagem_pil = pdf_bytes_para_imagem_pil(pdf_bytes)
        dados_ia = extrair_dados_estruturados_com_ia(imagem_pil, tipo_doc)
        
        resultado = {
            'codigo': normalizar_codigo_barras(dados_ia.get('codigo_barras_numerico')),
            'valor': normalizar_valor(dados_ia.get('valor_float')),
            'dados_completos': dados_ia,
            'origem': 'IA_GEMINI_ESTRUTURADO'
        }
        
        if resultado['valor'] == 0 and nome_arquivo:
            valor_nome = extrair_valor_nome(nome_arquivo)
            if valor_nome > 0:
                resultado['valor'] = valor_nome
                resultado['origem'] = 'NOME_ARQUIVO'
        
        return resultado
    except Exception as e:
        logger.error(f"Erro ao processar pÃƒÂ¡gina do PDF '{nome_arquivo}': {e}")
        valor_nome = extrair_valor_nome(nome_arquivo)
        return {'codigo': '', 'valor': valor_nome, 'dados_completos': {}, 'origem': 'ERRO_FATAL'}

def chamar_gemini_desempate(img_boleto, lista_imgs_comprovantes):
    """Usa IA para anÃƒÂ¡lise profunda e desempate."""
    logger.info(f"Acionando IA de desempate para {len(lista_imgs_comprovantes)} comprovantes.")
    model = genai.GenerativeModel(settings.GEMINI_MODEL)
    timeout_s = int(os.getenv('GEMINI_TIMEOUT_SECONDS', '300'))
    prompt_parts = [
        "Voce e um analista financeiro especialista em reconciliacao de boletos.",
        "Cenario critico: existem varios documentos com o MESMO VALOR.",
        "NUNCA escolha um candidato somente por valor.",
        "Compare o BOLETO com cada COMPROVANTE e decida com rigor.",
        "Criterios de maior peso: linha digitavel/codigo de barras, CNPJ beneficiario, nome beneficiario, CNPJ/nome pagador, data, banco, nosso numero, numero de documento.",
        "Se houver conflito em identificadores fortes, descarte o candidato.",
        "Se nao houver evidencia forte e suficiente para uma escolha segura, retorne -1.",
        "\n--- BOLETO ---", img_boleto, "\n--- COMPROVANTES CANDIDATOS ---",
    ]
    for i, img_comp in enumerate(lista_imgs_comprovantes):
        prompt_parts.extend([f"\nCANDIDATO INDICE {i}:", img_comp])
    prompt_parts.append("""
    Retorne APENAS um JSON no formato:
    {
      "melhor_indice_candidato": <numero ou -1>,
      "justificativa": "<resumo objetivo>",
      "sinais_fortes": ["<sinal 1>", "<sinal 2>"],
      "sinais_conflitantes": ["<conflito 1>", "<conflito 2>"],
      "confianca": <0.0 a 1.0>
    }

    Regras finais:
    - `melhor_indice_candidato` = -1 quando houver duvida material entre candidatos de mesmo valor.
    - Nao use markdown, nao use bloco de codigo.
    - Seja conservador: melhor retornar -1 do que escolher errado.
    """)
    try:
        response = gerar_conteudo_com_timeout(model, prompt_parts, timeout_s)
        if response is None:
            return {"melhor_indice_candidato": -1, "justificativa": "Timeout na IA."}
        texto_resposta = response.text.replace('```json', '').replace('```', '').strip()
        return json.loads(texto_resposta)
    except Exception as e:
        logger.error(f"Erro crÃƒÂ­tico na IA de desempate: {e}")
        return {"melhor_indice_candidato": -1, "justificativa": "Erro na IA."}

# ============================================================
# FLUXO PRINCIPAL DA RECONCILIAÃƒâ€¡ÃƒÆ’O (LÃƒâ€œGICA ATUALIZADA)
# ============================================================

def processar_reconciliacao(caminho_comprovantes, lista_caminhos_boletos, user):
    def emit(tipo, dados):
        return json.dumps({'type': tipo, 'data': dados}) + "\n"
    
    # FunÃƒÂ§ÃƒÂ£o auxiliar para formatar o log detalhado
    def formatar_log_extracao(dados, tipo, identificador):
        d = dados['dados_completos']
        valor = dados.get('valor', 0.0)
        data = d.get('data_pagamento') or d.get('data_vencimento') or 'N/A'
        pagador = d.get('nome_pagador', 'N/A')
        beneficiario = d.get('nome_beneficiario', 'N/A')
        codigo = dados.get('codigo', 'N/A')
        return (
            f"   -> {tipo} {identificador} | R${valor:.2f} | Data: {data} | "
            f"Pagador: {pagador} | BeneficiÃƒÂ¡rio: {beneficiario} | CÃƒÂ³d: {codigo}"
        )

    yield emit('log', 'Iniciando reconciliacao com extracao estruturada...')

    comprovantes_extraidos = []
    boletos_extraidos = []
    matches_resultado = []

    # --- ETAPA 1: LER COMPROVANTES ---
    yield emit('log', 'Lendo comprovantes...')
    pool_comprovantes = []
    try:
        doc_comprovantes = fitz.open(caminho_comprovantes)
        reader_zip = PdfReader(caminho_comprovantes)
        for i, page in enumerate(doc_comprovantes):
            writer = PdfWriter(); writer.add_page(reader_zip.pages[i]); bio = io.BytesIO(); writer.write(bio)
            pdf_bytes = bio.getvalue()
            time.sleep(1.5)
            dados_pagina = processar_pagina(pdf_bytes, "comprovante bancÃƒÂ¡rio")
            pool_comprovantes.append({
                'id': i, **dados_pagina,
                'pdf_bytes': pdf_bytes, 'usado': False
            })
            comprovantes_extraidos.append(serializar_extracao_item(pool_comprovantes[-1], 'comprovante'))
            yield emit('log', formatar_log_extracao(dados_pagina, "Comprovante", f"PÃƒÂ¡g {i+1}"))
            yield emit('comp_status', {'index': i, 'msg': f"R$ {dados_pagina['valor']:.2f}"})
    except Exception as e:
        yield emit('log', f"Ã¢ÂÅ’ Erro crÃƒÂ­tico ao ler comprovantes: {e}"); return

    # --- ETAPA 2: LER BOLETOS E COMBINAR ---
    yield emit('log', 'Analisando boletos e combinando...')
    lista_final_boletos = []
    for path_boleto in lista_caminhos_boletos:
        nome_arquivo = os.path.basename(path_boleto)
        yield emit('file_start', {'filename': nome_arquivo})
        try:
            with open(path_boleto, 'rb') as f: pdf_bytes_boleto = f.read()
            time.sleep(1)
            dados_boleto = processar_pagina(pdf_bytes_boleto, "boleto bancÃƒÂ¡rio", nome_arquivo)
            yield emit('log', formatar_log_extracao(dados_boleto, "Boleto", f'({nome_arquivo})'))

            boleto_atual = {
                'nome': nome_arquivo, **dados_boleto,
                'pdf_bytes': pdf_bytes_boleto, 'match': None,
                'motivo': 'Sem comprovante compatÃƒÂ­vel'
            }
            boletos_extraidos.append(serializar_extracao_item(boleto_atual, 'boleto'))
            
            candidatos = [c for c in pool_comprovantes if not c['usado']]
            melhor_candidato = None
            melhor_score = -1
            melhor_motivos = []
            for c in candidatos:
                score, motivos = calcular_score_match(boleto_atual, c)
                if score > melhor_score:
                    melhor_candidato = c
                    melhor_score = score
                    melhor_motivos = motivos

            candidatos_codigo = [
                c for c in candidatos
                if boleto_atual.get('codigo') and c.get('codigo') and codigos_sao_iguais(boleto_atual.get('codigo'), c.get('codigo'))
            ]
            candidatos_valor = [
                c for c in candidatos
                if valores_sao_iguais(boleto_atual.get('valor'), c.get('valor'))
            ]

            if len(candidatos_codigo) == 1:
                boleto_atual['match'] = candidatos_codigo[0]
                boleto_atual['match']['usado'] = True
                boleto_atual['motivo'] = "CODIGO DE BARRAS (UNICO)"
            elif len(candidatos_valor) == 1:
                boleto_atual['match'] = candidatos_valor[0]
                boleto_atual['match']['usado'] = True
                score_valor, motivos_valor = calcular_score_match(boleto_atual, candidatos_valor[0])
                boleto_atual['motivo'] = f"VALOR UNICO (score {score_valor}: {', '.join(motivos_valor)})"
            elif melhor_candidato and melhor_score >= 40:
                boleto_atual['match'] = melhor_candidato
                melhor_candidato['usado'] = True
                boleto_atual['motivo'] = f"SCORE {melhor_score} ({', '.join(melhor_motivos)})"
            elif melhor_candidato and melhor_score >= 20:
                # Ambiguo por score baixo: tenta IA apenas com top candidatos.
                top = sorted(
                    [(c, *calcular_score_match(boleto_atual, c)) for c in candidatos],
                    key=lambda x: x[1],
                    reverse=True
                )[:5]
                candidatos_ia = [x[0] for x in top]
                yield emit('log', f"   - Ambiguidade por score em {nome_arquivo}. Acionando IA com top {len(candidatos_ia)} candidatos...")
                img_boleto = pdf_bytes_para_imagem_pil(boleto_atual['pdf_bytes'])
                imgs = [pdf_bytes_para_imagem_pil(c['pdf_bytes']) for c in candidatos_ia]
                resultado_desempate = chamar_gemini_desempate(img_boleto, imgs)
                indice_escolhido = resultado_desempate.get('melhor_indice_candidato', -1)
                if isinstance(indice_escolhido, int) and 0 <= indice_escolhido < len(candidatos_ia):
                    boleto_atual['match'] = candidatos_ia[indice_escolhido]
                    boleto_atual['match']['usado'] = True
                    boleto_atual['motivo'] = f"IA ({resultado_desempate.get('justificativa')})"
                else:
                    boleto_atual['motivo'] = "AMBIGUO (score baixo e IA indecisa)"
            else:
                boleto_atual['motivo'] = "SEM CANDIDATO COM SCORE MINIMO"

            if not boleto_atual['match'] and not boleto_atual.get('codigo'):
                tolerancia_repasse = float(os.getenv('MATCH_TOLERANCIA_REPASSE', '20'))
                referencia_arquivo = extrair_referencia_nome_arquivo(nome_arquivo)
                candidatos_repasse = [
                    c for c in candidatos
                    if not c.get('codigo')
                    and referencia_aparece_no_texto(referencia_arquivo, c.get('dados_completos', {}).get('nome_beneficiario'))
                    and diferenca_valor(boleto_atual.get('valor'), c.get('valor')) <= tolerancia_repasse
                ]
                if candidatos_repasse:
                    escolhido = min(candidatos_repasse, key=lambda c: diferenca_valor(boleto_atual.get('valor'), c.get('valor')))
                    boleto_atual['match'] = escolhido
                    escolhido['usado'] = True
                    boleto_atual['motivo'] = (
                        f"REPASSE POR NOME+VALOR (ref {referencia_arquivo}, "
                        f"diff R${diferenca_valor(boleto_atual.get('valor'), escolhido.get('valor')):.2f})"
                    )

            if boleto_atual['match']:
                yield emit('log', f"   Ã¢Å“â€¦ COMBINADO: {nome_arquivo} -> Comprovante PÃƒÂ¡g {boleto_atual['match']['id']+1} (Motivo: {boleto_atual['motivo']})")
                yield emit('file_done', {'filename': nome_arquivo, 'status': 'success'})
            else:
                yield emit('log', f"   Ã¢Å¡Â Ã¯Â¸Â NÃƒÆ’O COMBINADO: {nome_arquivo}")
                yield emit('file_done', {'filename': nome_arquivo, 'status': 'warning'})
            lista_final_boletos.append(boleto_atual)
        except Exception as e:
            yield emit('log', f"Ã¢ÂÅ’ Erro no arquivo {nome_arquivo}: {e}")

    # --- ETAPA 2B: REANALISE DOS NAO COMBINADOS ---
    boletos_sem_match = [b for b in lista_final_boletos if not b.get('match')]
    if boletos_sem_match:
        yield emit('log', f"Rodada final: reanalisando {len(boletos_sem_match)} boletos sem match com comprovantes restantes.")
    recuperados_pos_analise = 0
    for boleto in boletos_sem_match:
        comprovantes_sem_match = [c for c in pool_comprovantes if not c.get('usado')]
        if not comprovantes_sem_match:
            break

        # Primeiro tenta reduzir o universo por valor para manter a IA focada.
        candidatos_finais = comprovantes_sem_match
        filtro_valor = False
        if boleto.get('valor', 0) > 0:
            candidatos_mesmo_valor = [
                c for c in comprovantes_sem_match
                if abs(c.get('valor', 0) - boleto['valor']) < 0.05
            ]
            if candidatos_mesmo_valor:
                candidatos_finais = candidatos_mesmo_valor
                filtro_valor = True

        # Se restou apenas um candidato claro, aproveita esse desempate final.
        if len(candidatos_finais) == 1:
            escolhido = candidatos_finais[0]
            boleto['match'] = escolhido
            boleto['motivo'] = "POS-VERIFICACAO (CANDIDATO UNICO RESTANTE)"
            escolhido['usado'] = True
            recuperados_pos_analise += 1
            yield emit('log', f"   COMBINADO (POS): {boleto['nome']} -> Comprovante Pag {escolhido['id']+1} (Candidato unico)")
            continue

        try:
            universo = "mesmo valor" if filtro_valor else "todos os restantes"
            yield emit('log', f"   Reanalise IA (POS): {boleto['nome']} com {len(candidatos_finais)} comprovantes ({universo}).")
            img_boleto = pdf_bytes_para_imagem_pil(boleto['pdf_bytes'])
            imgs_candidatos = [pdf_bytes_para_imagem_pil(c['pdf_bytes']) for c in candidatos_finais]
            resultado_pos = chamar_gemini_desempate(img_boleto, imgs_candidatos)
            indice_escolhido = resultado_pos.get('melhor_indice_candidato', -1)

            if isinstance(indice_escolhido, int) and 0 <= indice_escolhido < len(candidatos_finais):
                escolhido = candidatos_finais[indice_escolhido]
                boleto['match'] = escolhido
                boleto['motivo'] = f"IA POS-VERIFICACAO ({resultado_pos.get('justificativa')})"
                escolhido['usado'] = True
                recuperados_pos_analise += 1
                yield emit('log', f"   COMBINADO (POS): {boleto['nome']} -> Comprovante Pag {escolhido['id']+1}")
            else:
                yield emit('log', f"   POS sem match: {boleto['nome']} (IA sem confianca suficiente).")
        except Exception as e:
            yield emit('log', f"   Erro na reanalise POS de {boleto['nome']}: {e}")

    if recuperados_pos_analise > 0:
        yield emit('log', f"Reanalise POS concluiu com {recuperados_pos_analise} combinacoes recuperadas.")

    # --- ETAPA 3: GERAR ZIP ---
    yield emit('log', 'Montando o arquivo ZIP final...')
    matches_resultado = [
        {
            'boleto': serializar_extracao_item(boleto, 'boleto'),
            'comprovante': serializar_extracao_item(boleto['match'], 'comprovante') if boleto.get('match') else None,
            'status': 'match' if boleto.get('match') else 'sem_match',
            'motivo': boleto.get('motivo'),
        }
        for boleto in lista_final_boletos
    ]
    output_zip = io.BytesIO()
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr(
            "comprovantes_extraidos.json",
            json.dumps(comprovantes_extraidos, ensure_ascii=False, indent=2)
        )
        zip_file.writestr(
            "boletos_extraidos.json",
            json.dumps(boletos_extraidos, ensure_ascii=False, indent=2)
        )
        zip_file.writestr(
            "matches_resultado.json",
            json.dumps(matches_resultado, ensure_ascii=False, indent=2)
        )
        for boleto in lista_final_boletos:
            writer = PdfWriter()
            writer.append(io.BytesIO(boleto['pdf_bytes']))
            if boleto['match']:
                writer.append(io.BytesIO(boleto['match']['pdf_bytes']))
            pdf_combinado_bytes = io.BytesIO()
            writer.write(pdf_combinado_bytes)
            zip_file.writestr(boleto['nome'], pdf_combinado_bytes.getvalue())

    pasta_destino = os.path.join(settings.MEDIA_ROOT, 'downloads')
    os.makedirs(pasta_destino, exist_ok=True)
    nome_zip = f"Conciliacao_Final_{uuid.uuid4().hex[:8]}.zip"
    caminho_completo_zip = os.path.join(pasta_destino, nome_zip)
    with open(caminho_completo_zip, 'wb') as f:
        f.write(output_zip.getvalue())
    url_download = f"{settings.MEDIA_URL}downloads/{nome_zip}"
    yield emit('finish', {'url': url_download, 'total': len(lista_final_boletos)})
