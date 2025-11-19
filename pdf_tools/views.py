import os
import shutil
import json
from django.shortcuts import render
from django.http import JsonResponse, StreamingHttpResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
from django.core.files.storage import FileSystemStorage
from core.decorators import possui_produto
from .services import processar_conciliacao_json_stream
from datetime import datetime

# Função auxiliar para pegar a pasta do usuário
def get_user_temp_path(request):
    # Usa o username ou ID para criar uma pasta única
    return os.path.join(settings.MEDIA_ROOT, 'temp_staging', str(request.user.username))

@possui_produto('gerador-pdf')
def gerador_home(request):
    # Limpa a pasta se for um novo acesso (opcional, ou mantemos o estado)
    # Por enquanto, vamos listar o que já tem lá
    base_path = get_user_temp_path(request)
    
    arquivos = {'boletos': [], 'comprovantes': []}
    
    # Verifica arquivos já existentes na pasta
    for tipo in ['boletos', 'comprovantes']:
        path_tipo = os.path.join(base_path, tipo)
        if os.path.exists(path_tipo):
            arquivos[tipo] = os.listdir(path_tipo)

    return render(request, 'pdf_tools/explorer.html', {'arquivos': arquivos})

@csrf_exempt
def api_upload_arquivo(request):
    if request.method == 'POST':
        tipo = request.POST.get('tipo') # 'boletos' ou 'comprovantes'
        arquivo = request.FILES.get('file')
        
        if tipo not in ['boletos', 'comprovantes'] or not arquivo:
            return JsonResponse({'error': 'Dados inválidos'}, status=400)

        # Salva na pasta do usuário
        user_path = os.path.join(get_user_temp_path(request), tipo)
        os.makedirs(user_path, exist_ok=True)
        
        fs = FileSystemStorage(location=user_path)
        filename = fs.save(arquivo.name, arquivo)
        
        return JsonResponse({'status': 'ok', 'filename': filename, 'tipo': tipo})
    return JsonResponse({'error': 'POST required'}, status=400)

@csrf_exempt
def api_delete_arquivo(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        tipo = data.get('tipo')
        filename = data.get('filename')
        
        path = os.path.join(get_user_temp_path(request), tipo, filename)
        if os.path.exists(path):
            os.remove(path)
            return JsonResponse({'status': 'deleted'})
            
    return JsonResponse({'error': 'File not found'}, status=404)

def api_iniciar_processamento(request):
    # Pega os caminhos
    base_path = get_user_temp_path(request)
    path_boletos = os.path.join(base_path, 'boletos')
    path_comprovantes = os.path.join(base_path, 'comprovantes')
    
    # Verifica se tem arquivos
    if not os.path.exists(path_boletos) or not os.listdir(path_boletos):
        return HttpResponse(json.dumps({'error': 'Nenhum boleto encontrado'}), content_type="application/json")
        
    # Verifica se tem comprovante (pelo menos 1 arquivo na pasta comprovantes)
    arquivos_comp = os.listdir(path_comprovantes) if os.path.exists(path_comprovantes) else []
    if not arquivos_comp:
        return HttpResponse(json.dumps({'error': 'Nenhum comprovante encontrado'}), content_type="application/json")
    
    # Pega o caminho completo do primeiro comprovante (regra atual: 1 arquivo PDF com varias paginas)
    caminho_comp_completo = os.path.join(path_comprovantes, arquivos_comp[0])
    
    # Lista completa dos boletos
    lista_boletos = [os.path.join(path_boletos, f) for f in os.listdir(path_boletos) if f.endswith('.pdf')]
    
    # Inicia o Stream JSON
    response = StreamingHttpResponse(
        processar_conciliacao_json_stream(lista_boletos, caminho_comp_completo, request.user),
        content_type='application/x-ndjson' # Formato especial para stream JSON
    )
    return response