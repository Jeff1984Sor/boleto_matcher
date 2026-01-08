from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from .models import TermoTemplate, TermoAssinado
from cadastros_fit.models import Aluno
from django.utils import timezone

def gerar_termo_aluno(request, aluno_id):
    if request.method == "POST":
        aluno = get_object_or_404(Aluno, id=aluno_id)
        template_id = request.POST.get('template_id')
        template = get_object_or_404(TermoTemplate, id=template_id)
        
        # Cria o termo pendente
        termo = TermoAssinado.objects.create(
            aluno=aluno,
            template=template
        )
        return redirect('aluno_detail', pk=aluno.id)

def assinar_termo(request, token):
    termo = get_object_or_404(TermoAssinado, token_assinatura=token)
    
    if request.method == "POST":
        # Lógica para salvar a imagem da assinatura (Base64)
        assinatura_data = request.POST.get('assinatura_data')
        termo.assinatura_imagem = assinatura_data
        termo.data_assinatura = timezone.now()
        termo.ip_assinatura = request.META.get('REMOTE_ADDR')
        termo.save()
        return JsonResponse({'status': 'ok'})

    # Preenche as variáveis do template (Ex: nome do aluno)
    texto_final = termo.template.texto_html.replace('{{ aluno_nome }}', termo.aluno.nome)
    
    return render(request, 'termos_fit/assinar_termo.html', {
        'termo': termo,
        'texto_contrato': texto_final
    })

def termo_template_list(request):
    templates = TermoTemplate.objects.filter(ativo=True)
    return render(request, 'termos_fit/termo_template_list.html', {
        'templates': templates
    })