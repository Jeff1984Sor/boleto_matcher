from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from .models import TermoTemplate, TermoAssinado
from cadastros_fit.models import Aluno
from django.utils import timezone
from .forms import TermoTemplateForm

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
        assinatura_data = request.POST.get('assinatura_data')
        termo.assinatura_imagem = assinatura_data
        termo.data_assinatura = timezone.now()
        termo.ip_assinatura = request.META.get('REMOTE_ADDR')
        termo.save()
        return JsonResponse({'status': 'ok'})

    # Texto original do template
    texto_final = termo.template.texto_html
    aluno = termo.aluno

    # Mapeamento completo baseado na sua model de Aluno
    substituicoes = {
        '[[ALUNO_NOME]]': aluno.nome,
        '[[ALUNO_CPF]]': aluno.cpf if aluno.cpf else "___.___.___-__",
        '[[ALUNO_DATA_NASC]]': aluno.data_nascimento.strftime('%d/%m/%Y') if aluno.data_nascimento else "__/__/____",
        '[[ALUNO_EMAIL]]': aluno.email,
        '[[ALUNO_TELEFONE]]': aluno.telefone,
        '[[ALUNO_ENDERECO]]': aluno.endereco_completo, # Usando a @property que você criou
        '[[DATA_HOJE]]': timezone.now().strftime('%d de %B de %Y'),
        '[[NOME_UNIDADE]]': "Mayacorp Fit", # Aqui você pode buscar de Unidade se houver relação
    }

    # Faz a troca de todas as tags
    for tag, valor in substituicoes.items():
        texto_final = texto_final.replace(tag, str(valor))
    
    return render(request, 'termos_fit/assinar_termo.html', {
        'termo': termo,
        'texto_contrato': texto_final
    })
def termo_template_list(request):
    templates = TermoTemplate.objects.filter(ativo=True)
    return render(request, 'termos_fit/termo_template_list.html', {
        'templates': templates
    })

def termo_template_create(request):
    if request.method == "POST":
        form = TermoTemplateForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect('termo_template_list')
    else:
        form = TermoTemplateForm()
    return render(request, 'termos_fit/termo_template_form.html', {'form': form})