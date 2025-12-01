from django import forms
from django.forms import inlineformset_factory
from django.core.exceptions import ValidationError
from django.forms.models import BaseInlineFormSet
from .models import Contrato, HorarioFixo, Plano

# 1. Classe de Validação Customizada (Sua lógica entra aqui)
class BaseHorarioFixoFormSet(BaseInlineFormSet):
    def clean(self):
        super().clean()
        
        # Se houver erros nos formulários filhos (ex: campo vazio), para aqui
        if any(self.errors):
            return

        # Conta quantos horários estão sendo salvos (ignorando os marcados para deletar)
        count = 0
        for form in self.forms:
            if form.cleaned_data and not form.cleaned_data.get('DELETE', False):
                count += 1
        
        # Pega a instância do contrato (pai)
        contrato = self.instance
        
        # Validação da Frequência
        # Nota: Isso depende que na View você faça contrato = form.save(commit=False) antes de validar o formset
        if contrato.plano:
            if count > contrato.plano.frequencia_semanal:
                raise ValidationError(
                    f"O plano '{contrato.plano.nome}' permite apenas {contrato.plano.frequencia_semanal} horários por semana. "
                    f"Você tentou adicionar {count}."
                )
            # Opcional: Validar se selecionou MENOS horários que o permitido
            # elif count < contrato.plano.frequencia_semanal:
            #     raise ValidationError(f"O plano exige {contrato.plano.frequencia_semanal} horários. Você selecionou apenas {count}.")

# 2. Formulário do Contrato (Pai) com Bootstrap
class ContratoForm(forms.ModelForm):
    class Meta:
        model = Contrato
        fields = ['plano', 'unidade', 'data_inicio', 'valor_total', 'qtde_parcelas', 'dia_vencimento']
        
        widgets = {
            'data_inicio': forms.DateInput(attrs={'type': 'date', 'class': 'form-control'}),
            'plano': forms.Select(attrs={'class': 'form-select', 'id': 'select_plano'}),
            'unidade': forms.Select(attrs={'class': 'form-select'}),
            'valor_total': forms.NumberInput(attrs={'class': 'form-control', 'step': '0.01', 'placeholder': 'Ex: 1200.00'}),
            'qtde_parcelas': forms.NumberInput(attrs={'class': 'form-control'}),
            'dia_vencimento': forms.NumberInput(attrs={'class': 'form-control', 'max': 31, 'min': 1}),
        }
        
        labels = {
            'data_inicio': 'Data de Início',
            'valor_total': 'Valor Total Fechado (R$)',
            'qtde_parcelas': 'Nº de Parcelas',
            'dia_vencimento': 'Dia do Vencimento'
        }

# 3. Factory do Formset (Juntando a validação com os Widgets)
HorarioFixoFormSet = inlineformset_factory(
    Contrato, 
    HorarioFixo,
    formset=BaseHorarioFixoFormSet, # <--- Aqui conectamos a sua classe de validação
    fields=['dia_semana', 'horario', 'profissional'],
    extra=2, # Começa com 2 linhas (pode ser ajustado via JS)
    can_delete=True,
    widgets={
        'dia_semana': forms.Select(attrs={'class': 'form-select'}),
        'horario': forms.TimeInput(attrs={'type': 'time', 'class': 'form-control'}),
        'profissional': forms.Select(attrs={'class': 'form-select'}),
    }
)

class PlanoForm(forms.ModelForm):
    class Meta:
        model = Plano
        # Se você ainda tiver 'organizacao' no model, exclua aqui
        exclude = ['organizacao', 'ativo'] 
        
        widgets = {
            'nome': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Ex: Pilates 2x - Trimestral'}),
            'valor_mensal': forms.NumberInput(attrs={'class': 'form-control', 'placeholder': '0.00'}),
            'frequencia_semanal': forms.NumberInput(attrs={'class': 'form-control'}),
            'duracao_meses': forms.Select(attrs={'class': 'form-select'}),
        }