from django import forms
from .models import Aluno, Profissional, Unidade

from .models import DocumentoAluno

class AlunoForm(forms.ModelForm):
    class Meta:
        model = Aluno
        # Removemos 'organizacao' e campos automáticos
        exclude = ['organizacao', 'criado_em', 'biometria_template']
        widgets = {
            'data_nascimento': forms.DateInput(attrs={'type': 'date'}),
            'anamnese': forms.Textarea(attrs={'rows': 4, 'placeholder': 'Histórico de lesões, cirurgias, dores...'}),
        }

class ProfissionalForm(forms.ModelForm):
    class Meta:
        model = Profissional
        exclude = ['organizacao']
        widgets = {
            # Input especial de cor HTML5
            'cor_agenda': forms.TextInput(attrs={'type': 'color', 'style': 'height: 40px; width: 100px;'}),
        }

class UnidadeForm(forms.ModelForm):
    class Meta:
        model = Unidade
        exclude = ['organizacao']

class DocumentoExtraForm(forms.ModelForm):
    class Meta:
        model = DocumentoAluno
        fields = ['titulo', 'arquivo']