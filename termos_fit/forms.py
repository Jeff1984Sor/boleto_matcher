from django import forms
from .models import TermoTemplate

class TermoTemplateForm(forms.ModelForm):
    class Meta:
        model = TermoTemplate
        fields = ['nome', 'tipo', 'texto_html', 'ativo']
        widgets = {
            'nome': forms.TextInput(attrs={'class': 'form-control'}),
            'tipo': forms.Select(attrs={'class': 'form-control'}),
            'texto_html': forms.Textarea(attrs={'class': 'form-control', 'rows': 10}),
            'ativo': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        }