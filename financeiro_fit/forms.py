from django import forms
from .models import CategoriaFinanceira, ContaBancaria, Lancamento # <--- Importante!

class CategoriaForm(forms.ModelForm):
    class Meta:
        model = CategoriaFinanceira
        # ADICIONE ESTA LINHA:
        fields = ['nome', 'tipo', 'categoria_pai']
        
        widgets = {
            'nome': forms.TextInput(attrs={'class': 'form-control'}),
            'tipo': forms.Select(attrs={'class': 'form-select'}),
            'categoria_pai': forms.Select(attrs={'class': 'form-select'}),
        }

class ContaBancariaForm(forms.ModelForm):
    class Meta:
        model = ContaBancaria
        fields = ['nome', 'saldo_atual'] # <--- Defina os campos aqui
        widgets = {
            'nome': forms.TextInput(attrs={'class': 'form-control'}),
            'saldo_atual': forms.NumberInput(attrs={'class': 'form-control', 'step': '0.01'}),
        }

class DespesaForm(forms.ModelForm):
    class Meta:
        model = Lancamento
        fields = ['descricao', 'categoria', 'conta', 'valor', 'data_vencimento', 'status']
        widgets = {
            'descricao': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Ex: Conta de Luz'}),
            'categoria': forms.Select(attrs={'class': 'form-select'}),
            'conta': forms.Select(attrs={'class': 'form-select'}),
            'valor': forms.NumberInput(attrs={'class': 'form-control', 'step': '0.01'}),
            'data_vencimento': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'status': forms.Select(attrs={'class': 'form-select'}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Filtra apenas categorias de DESPESA
        self.fields['categoria'].queryset = CategoriaFinanceira.objects.filter(tipo='DESPESA')