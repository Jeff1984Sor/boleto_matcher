from django import forms
from .models import CategoriaFinanceira, ContaBancaria, Lancamento, Fornecedor

# ==============================================================================
# 1. CADASTROS BÁSICOS
# ==============================================================================

class FornecedorForm(forms.ModelForm):
    class Meta:
        model = Fornecedor
        fields = ['nome', 'nome_fantasia', 'cnpj_cpf', 'telefone', 'email', 'chave_pix', 'ativo']
        widgets = {
            'nome': forms.TextInput(attrs={'class': 'form-control'}),
            'nome_fantasia': forms.TextInput(attrs={'class': 'form-control'}),
            'cnpj_cpf': forms.TextInput(attrs={'class': 'form-control'}),
            'telefone': forms.TextInput(attrs={'class': 'form-control'}),
            'email': forms.EmailInput(attrs={'class': 'form-control'}),
            'chave_pix': forms.TextInput(attrs={'class': 'form-control'}),
            'ativo': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        }

class CategoriaForm(forms.ModelForm):
    class Meta:
        model = CategoriaFinanceira
        fields = ['nome', 'tipo', 'categoria_pai']
        widgets = {
            'nome': forms.TextInput(attrs={'class': 'form-control'}),
            'tipo': forms.Select(attrs={'class': 'form-select'}),
            'categoria_pai': forms.Select(attrs={'class': 'form-select'}),
        }

class ContaBancariaForm(forms.ModelForm):
    class Meta:
        model = ContaBancaria
        fields = ['nome', 'saldo_atual']
        widgets = {
            'nome': forms.TextInput(attrs={'class': 'form-control'}),
            'saldo_atual': forms.NumberInput(attrs={'class': 'form-control', 'step': '0.01'}),
        }

# ==============================================================================
# 2. LANÇAMENTO DE DESPESA (CONTAS A PAGAR)
# ==============================================================================

class DespesaForm(forms.ModelForm):
    # --- CAMPOS VIRTUAIS PARA RECORRÊNCIA ---
    # Esses campos não existem no banco, mas usamos na View para criar múltiplas contas
    repetir = forms.BooleanField(required=False, label="É uma conta recorrente?", widget=forms.CheckboxInput(attrs={'class': 'form-check-input', 'onchange': 'toggleRecorrencia()'}))
    
    frequencia = forms.ChoiceField(
        choices=[('MENSAL', 'Mensal'), ('SEMANAL', 'Semanal'), ('ANUAL', 'Anual')],
        required=False, 
        label="Frequência",
        widget=forms.Select(attrs={'class': 'form-select'})
    )
    
    qtd_repeticoes = forms.IntegerField(
        min_value=2, max_value=60, required=False, label="Quantas vezes?", initial=12,
        widget=forms.NumberInput(attrs={'class': 'form-control'})
    )

    class Meta:
        model = Lancamento
        fields = [
            'descricao', 'fornecedor', 'profissional', 
            'categoria', 'conta', 'valor', 'data_vencimento', 
            'arquivo_boleto', 'arquivo_comprovante', 'status'
        ]
        
        widgets = {
            'descricao': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Ex: Aluguel'}),
            'fornecedor': forms.Select(attrs={'class': 'form-select'}),
            'profissional': forms.Select(attrs={'class': 'form-select'}),
            'categoria': forms.Select(attrs={'class': 'form-select'}),
            'conta': forms.Select(attrs={'class': 'form-select'}),
            'valor': forms.NumberInput(attrs={'class': 'form-control', 'step': '0.01'}),
            'data_vencimento': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'arquivo_boleto': forms.FileInput(attrs={'class': 'form-control'}),
            'arquivo_comprovante': forms.FileInput(attrs={'class': 'form-control'}),
            'status': forms.Select(attrs={'class': 'form-select'}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Filtra apenas categorias de DESPESA para não misturar
        self.fields['categoria'].queryset = CategoriaFinanceira.objects.filter(tipo='DESPESA')
        # Labels mais amigáveis
        self.fields['fornecedor'].empty_label = "Selecione um Fornecedor (Opcional)"
        self.fields['profissional'].empty_label = "Selecione um Profissional (Se for Salário)"