from django import forms
from django.forms import inlineformset_factory
from django.core.exceptions import ValidationError
from django.forms.models import BaseInlineFormSet
from .models import Contrato, HorarioFixo, Plano

# 1. Validação Customizada dos Horários
class BaseHorarioFixoFormSet(BaseInlineFormSet):
    def clean(self):
        super().clean()
        if any(self.errors): return

        count = 0
        for form in self.forms:
            if form.cleaned_data and not form.cleaned_data.get('DELETE', False):
                count += 1
        
        contrato = self.instance
        
        if contrato.plano:
            # Garante que não salvou mais horários do que o permitido
            if count > contrato.plano.frequencia_semanal:
                raise ValidationError(
                    f"O plano '{contrato.plano.nome}' permite apenas {contrato.plano.frequencia_semanal} horários. "
                    f"Você preencheu {count}."
                )

# 2. Formulário do Contrato
class ContratoForm(forms.ModelForm):
    # Campo visual (não salva no banco direto) para mostrar ao usuário quando acaba
    data_encerramento = forms.DateField(
        label="Data Prevista Encerramento",
        required=False,
        widget=forms.DateInput(attrs={
            'class': 'form-control', 
            'readonly': 'readonly', 
            'id': 'id_data_encerramento',
            'style': 'background-color: #e9ecef;'
        })
    )

    class Meta:
        model = Contrato
        fields = ['plano', 'unidade', 'data_inicio', 'valor_total', 'qtde_parcelas', 'dia_vencimento']
        
        widgets = {
            'data_inicio': forms.DateInput(attrs={'type': 'date', 'class': 'form-control', 'id': 'id_data_inicio'}),
            'plano': forms.Select(attrs={'class': 'form-select', 'id': 'select_plano'}),
            'unidade': forms.Select(attrs={'class': 'form-select'}),
            'valor_total': forms.NumberInput(attrs={'class': 'form-control', 'step': '0.01', 'id': 'id_valor_total'}),
            'qtde_parcelas': forms.NumberInput(attrs={'class': 'form-control', 'id': 'id_qtde_parcelas'}),
            'dia_vencimento': forms.NumberInput(attrs={'class': 'form-control', 'max': 31, 'min': 1, 'id': 'id_dia_vencimento'}),
        }
        
        labels = {
            'data_inicio': 'Data de Início',
            'valor_total': 'Valor Total Fechado (R$)',
            'qtde_parcelas': 'Nº de Parcelas',
            'dia_vencimento': 'Dia do Vencimento'
        }

# 3. Formset dos Horários
HorarioFixoFormSet = inlineformset_factory(
    Contrato, 
    HorarioFixo,
    formset=BaseHorarioFixoFormSet,
    fields=['dia_semana', 'horario', 'profissional'],
    extra=7, # Mudei para 7 (para cobrir todos os dias da semana se necessário)
    can_delete=True,
    widgets={
        'dia_semana': forms.Select(attrs={'class': 'form-select'}),
        'horario': forms.TimeInput(attrs={'type': 'time', 'class': 'form-control'}),
        'profissional': forms.Select(attrs={'class': 'form-select'}),
    }
)

# 4. Cadastro de Planos
class PlanoForm(forms.ModelForm):
    class Meta:
        model = Plano
        # O 'exclude' garante que todos os outros campos do Model (incluindo descricao) 
        # sejam incluídos no formulário, exceto 'organizacao' e 'ativo'.
        exclude = ['organizacao', 'ativo'] 
        
        widgets = {
            'nome': forms.TextInput(attrs={
                'class': 'form-control', 
                'placeholder': 'Ex: Pilates 2x - Trimestral'
            }),
            'descricao': forms.Textarea(attrs={
                'class': 'form-control', 
                'placeholder': 'Descreva os detalhes e benefícios deste plano...',
                'rows': 3  # Ajusta a altura da caixa de texto
            }),
            'valor_mensal': forms.NumberInput(attrs={
                'class': 'form-control', 
                'placeholder': '0.00'
            }),
            'frequencia_semanal': forms.NumberInput(attrs={
                'class': 'form-control'
            }),
            'duracao_meses': forms.Select(attrs={
                'class': 'form-select'
            }),
        }