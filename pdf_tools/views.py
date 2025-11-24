import os
import shutil
import json
from django.shortcuts import render
from django.http import JsonResponse, StreamingHttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
from django.core.files.storage import FileSystemStorage
from core.decorators import possui_produto
from .services import processar_reconciliacao

# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def get_user_temp_path(request):
    """
    Retorna o caminho da pasta temporária do usuário.
    Formato: MEDIA_ROOT/temp_staging/username/
    """
    return os.path.join(settings.MEDIA_ROOT, 'temp_staging', str(request.user.username))

# ============================================================
# VIEWS
# ============================================================

@possui_produto('gerador-pdf')
def gerador_home(request):
    """
    View principal que lista arquivos já enviados.
    """
    base_path = get_user_temp_path(request)
    
    arquivos = {'boletos': [], 'comprovantes': []}
    
    # Listar arquivos por tipo
    for tipo in ['boletos', 'comprovantes']:
        path_tipo = os.path.join(base_path, tipo)
        if os.path.exists(path_tipo):
            # Filtra apenas PDFs
            arquivos[tipo] = [f for f in os.listdir(path_tipo) if f.endswith('.pdf')]
    
    return render(request, 'pdf_tools/explorer.html', {'arquivos': arquivos})

# ============================================================
# API: UPLOAD DE ARQUIVO
# ============================================================

@csrf_exempt
def api_upload_arquivo(request):
    """
    Upload de arquivo (boleto ou comprovante).
    
    POST /api/upload/
    Params:
      - tipo: 'boletos' ou 'comprovantes'
      - file: arquivo PDF
    """
    if request.method != 'POST':
        return JsonResponse({'error': 'Método não permitido'}, status=405)
    
    tipo = request.POST.get('tipo')
    arquivo = request.FILES.get('file')
    
    # Validações
    if tipo not in ['boletos', 'comprovantes']:
        return JsonResponse({'error': 'Tipo inválido. Use "boletos" ou "comprovantes"'}, status=400)
    
    if not arquivo:
        return JsonResponse({'error': 'Nenhum arquivo enviado'}, status=400)
    
    if not arquivo.name.lower().endswith('.pdf'):
        return JsonResponse({'error': 'Apenas arquivos PDF são permitidos'}, status=400)
    
    try:
        # Criar pasta se não existir
        user_path = os.path.join(get_user_temp_path(request), tipo)
        os.makedirs(user_path, exist_ok=True)
        
        # Salvar arquivo
        fs = FileSystemStorage(location=user_path)
        filename = fs.save(arquivo.name, arquivo)
        
        return JsonResponse({
            'status': 'ok',
            'filename': filename,
            'tipo': tipo,
            'mensagem': f'Arquivo "{filename}" enviado com sucesso'
        })
    
    except Exception as e:
        return JsonResponse({'error': f'Erro ao salvar arquivo: {str(e)}'}, status=500)

# ============================================================
# API: DELETE DE ARQUIVO
# ============================================================

@csrf_exempt
def api_delete_arquivo(request):
    """
    Deleta um arquivo específico.
    
    POST /api/delete/
    Body JSON:
      - tipo: 'boletos' ou 'comprovantes'
      - filename: nome do arquivo
    """
    if request.method != 'POST':
        return JsonResponse({'error': 'Método não permitido'}, status=405)
    
    try:
        data = json.loads(request.body)
        tipo = data.get('tipo')
        filename = data.get('filename')
        
        # Validações
        if tipo not in ['boletos', 'comprovantes']:
            return JsonResponse({'error': 'Tipo inválido'}, status=400)
        
        if not filename:
            return JsonResponse({'error': 'Filename não informado'}, status=400)
        
        # Segurança: usar apenas basename para evitar path traversal
        safe_filename = os.path.basename(filename)
        path = os.path.join(get_user_temp_path(request), tipo, safe_filename)
        
        # Verificar se arquivo existe
        if not os.path.exists(path):
            return JsonResponse({'error': 'Arquivo não encontrado'}, status=404)
        
        # Deletar
        os.remove(path)
        
        return JsonResponse({
            'status': 'deleted',
            'mensagem': f'Arquivo "{filename}" deletado'
        })
    
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Body JSON inválido'}, status=400)
    except Exception as e:
        return JsonResponse({'error': f'Erro ao deletar: {str(e)}'}, status=500)

# ============================================================
# API: INICIAR PROCESSAMENTO (STREAM)
# ============================================================

def api_iniciar_processamento(request):
    """
    Inicia o processamento e retorna stream de logs (NDJSON).
    
    GET /api/processar/
    
    Validações:
    - Deve ter pelo menos 1 boleto
    - Deve ter exatamente 1 arquivo de comprovantes
    
    Response: Stream NDJSON com logs e resultado final
    """
    base_path = get_user_temp_path(request)
    path_boletos = os.path.join(base_path, 'boletos')
    path_comprovantes = os.path.join(base_path, 'comprovantes')
    
    # ========================================================
    # VALIDAÇÕES
    # ========================================================
    
    # 1. Validar boletos
    if not os.path.exists(path_boletos):
        return JsonResponse({
            'error': 'Pasta de boletos não existe. Faça o upload primeiro.'
        }, status=400)
    
    arquivos_boletos = [f for f in os.listdir(path_boletos) if f.endswith('.pdf')]
    
    if not arquivos_boletos:
        return JsonResponse({
            'error': 'Nenhum boleto foi enviado. Envie pelo menos 1 arquivo PDF.'
        }, status=400)
    
    # 2. Validar comprovantes
    if not os.path.exists(path_comprovantes):
        return JsonResponse({
            'error': 'Pasta de comprovantes não existe. Envie o arquivo de comprovantes.'
        }, status=400)
    
    arquivos_comprovantes = [f for f in os.listdir(path_comprovantes) if f.endswith('.pdf')]
    
    if not arquivos_comprovantes:
        return JsonResponse({
            'error': 'Nenhum arquivo de comprovantes foi enviado.'
        }, status=400)
    
    if len(arquivos_comprovantes) > 1:
        return JsonResponse({
            'error': f'Envie apenas 1 arquivo de comprovantes. Você enviou {len(arquivos_comprovantes)}.'
        }, status=400)
    
    # ========================================================
    # PREPARAR CAMINHOS
    # ========================================================
    
    caminho_comp_completo = os.path.join(path_comprovantes, arquivos_comprovantes[0])
    lista_boletos = [os.path.join(path_boletos, f) for f in arquivos_boletos]
    
    print("\n" + "="*70)
    print("INICIANDO PROCESSAMENTO")
    print("="*70)
    print(f"Arquivo de comprovantes: {arquivos_comprovantes[0]}")
    print(f"Total de boletos: {len(lista_boletos)}")
    print("="*70 + "\n")
    
    # ========================================================
    # INICIAR STREAM
    # ========================================================
    
    try:
        response = StreamingHttpResponse(
            processar_reconciliacao(
                caminho_comprovantes=caminho_comp_completo,
                lista_caminhos_boletos=lista_boletos,
                user=request.user
            ),
            content_type='application/x-ndjson'
        )
        
        # Headers importantes
        response['Cache-Control'] = 'no-cache'
        response['X-Accel-Buffering'] = 'no'  # Para Nginx
        
        return response
    
    except Exception as e:
        print(f"\n❌ ERRO AO INICIAR STREAM: {str(e)}\n")
        return JsonResponse({
            'error': f'Erro ao iniciar processamento: {str(e)}'
        }, status=500)

# ============================================================
# API: LIMPAR TUDO
# ============================================================

@csrf_exempt
def api_limpar_tudo(request):
    """
    Limpa todos os arquivos do usuário.
    
    POST /api/limpar/
    """
    if request.method != 'POST':
        return JsonResponse({'error': 'Método não permitido'}, status=405)
    
    try:
        base_path = get_user_temp_path(request)
        
        if os.path.exists(base_path):
            shutil.rmtree(base_path)
        
        # Recriar pastas vazias
        os.makedirs(os.path.join(base_path, 'boletos'), exist_ok=True)
        os.makedirs(os.path.join(base_path, 'comprovantes'), exist_ok=True)
        
        return JsonResponse({
            'status': 'ok',
            'mensagem': 'Todos os arquivos foram deletados'
        })
    
    except Exception as e:
        return JsonResponse({
            'error': f'Erro ao limpar: {str(e)}'
        }, status=500)

# ============================================================
# API: LISTAR ARQUIVOS
# ============================================================

@csrf_exempt
def api_listar_arquivos(request):
    """
    Lista todos os arquivos do usuário.
    
    GET /api/listar/
    
    Response:
    {
      "boletos": ["arquivo1.pdf", "arquivo2.pdf"],
      "comprovantes": ["comprovante.pdf"]
    }
    """
    try:
        base_path = get_user_temp_path(request)
        
        arquivos = {'boletos': [], 'comprovantes': []}
        
        for tipo in ['boletos', 'comprovantes']:
            path_tipo = os.path.join(base_path, tipo)
            if os.path.exists(path_tipo):
                arquivos[tipo] = [
                    f for f in os.listdir(path_tipo) 
                    if f.endswith('.pdf')
                ]
        
        return JsonResponse({
            'status': 'ok',
            'arquivos': arquivos
        })
    
    except Exception as e:
        return JsonResponse({
            'error': f'Erro ao listar: {str(e)}'
        }, status=500)