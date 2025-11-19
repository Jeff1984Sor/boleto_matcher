import os
import uuid
import shutil
from django.shortcuts import render
from django.http import HttpResponse
from django.conf import settings
from core.decorators import possui_produto
from .services import processar_conciliacao
from datetime import datetime
from django.core.files.storage import FileSystemStorage

@possui_produto('gerador-pdf')
def gerador_home(request):
    if request.method == "POST":
        boletos = request.FILES.getlist('boletos')
        comprovantes = request.FILES.get('comprovantes')
        
        if boletos and comprovantes:
            # 1. Criar uma pasta temporária única para essa operação
            # Ex: media/temp/a1b2c3d4...
            operacao_id = str(uuid.uuid4())
            caminho_temp = os.path.join(settings.MEDIA_ROOT, 'temp', operacao_id)
            os.makedirs(caminho_temp, exist_ok=True)
            
            fs = FileSystemStorage(location=caminho_temp)
            
            try:
                # 2. Salvar os Boletos no disco
                lista_caminhos_boletos = []
                for bol in boletos:
                    filename = fs.save(bol.name, bol)
                    lista_caminhos_boletos.append(fs.path(filename))
                
                # 3. Salvar o Comprovante no disco
                filename_comp = fs.save(comprovantes.name, comprovantes)
                caminho_comprovante = fs.path(filename_comp)
                
                # 4. Chamar o Serviço passando os CAMINHOS (Paths) e não mais os objetos
                zip_buffer, qtd_paginas = processar_conciliacao(lista_caminhos_boletos, caminho_comprovante)
                
                # 5. Atualizar Créditos
                request.user.paginas_processadas += qtd_paginas
                request.user.save()
                
                # 6. Preparar Download
                hoje = datetime.now().strftime("%d-%m-%Y")
                nome_do_zip = f"Boletos + Comprovantes - {hoje}.zip"
                
                response = HttpResponse(zip_buffer, content_type='application/zip')
                response['Content-Disposition'] = f'attachment; filename="{nome_do_zip}"'
                
                return response

            except Exception as e:
                # Se der erro, mostra na tela (bom pra debug em produção)
                return HttpResponse(f"Erro no processamento: {str(e)}", status=500)
            
            finally:
                # 7. LIMPEZA: Apaga a pasta temporária inteira (sucesso ou erro)
                # Se quiser guardar por dias, é só comentar essa linha, mas cuidado com espaço em disco!
                if os.path.exists(caminho_temp):
                    shutil.rmtree(caminho_temp)

    return render(request, 'pdf_tools/index.html')