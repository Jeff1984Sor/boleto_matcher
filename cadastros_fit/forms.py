from django import forms
from .models import Aluno, Profissional, Unidade, DocumentoAluno

class AlunoForm(forms.ModelForm):
    class Meta:
        model = Aluno
        # Excluímos o que o usuário não deve mexer manualmente
        exclude = ['organizacao', 'criado_em', 'atualizado_em', 'biometria_template', 'ativo']
        
        widgets = {
            # Datas
            'data_nascimento': forms.DateInput(attrs={'type': 'date', 'class': 'form-control'}),
            
            # Textos e Máscaras (Placeholder ajuda a saber o formato)
            'nome': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Nome Completo'}),
            'cpf': forms.TextInput(attrs={'class': 'form-control', 'placeholder': '000.000.000-00'}),
            'telefone': forms.TextInput(attrs={'class': 'form-control', 'placeholder': '(00) 90000-0000'}),
            'email': forms.EmailInput(attrs={'class': 'form-control', 'placeholder': 'email@exemplo.com'}),
            
            # Endereço
            'cep': forms.TextInput(attrs={
                'class': 'form-control', 
                'placeholder': '00000-000', 
                'onblur': 'buscarCep(this.value)'  # <--- Gatilho futuro se quiser consulta de CEP
            }),
            'logradouro': forms.TextInput(attrs={'class': 'form-control'}),
            'numero': forms.TextInput(attrs={'class': 'form-control'}),
            'complemento': forms.TextInput(attrs={'class': 'form-control'}),
            'bairro': forms.TextInput(attrs={'class': 'form-control'}),
            'cidade': forms.TextInput(attrs={'class': 'form-control'}),
            'estado': forms.TextInput(attrs={'class': 'form-control', 'maxlength': '2', 'style': 'text-transform:uppercase'}),
            
            # Uploads
            'foto_rosto': forms.FileInput(attrs={'class': 'form-control'}),
            'doc_identidade_foto': forms.FileInput(attrs={'class': 'form-control'}),
            'comprovante_residencia_foto': forms.FileInput(attrs={'class': 'form-control'}),
            
            # Áreas de Texto
            'anamnese': forms.Textarea(attrs={'class': 'form-control', 'rows': 4}), # Caso use JSON, ou Textarea se mudou para TextField
            # Se for JSONField no model, o Django admin usa um widget específico, mas no front podemos usar Textarea se for simples
        }
    
    # Se 'anamnese' for JSONField no model, mas você quer editar como texto no form:
    anamnese = forms.CharField(
        widget=forms.Textarea(attrs={'class': 'form-control', 'rows': 4, 'placeholder': 'Histórico médico, dores, cirurgias...'}),
        required=False
    )

    def clean_anamnese(self):
        # Garante que salve um JSON válido ou string simples dentro de um dict
        data = self.cleaned_data['anamnese']
        if isinstance(data, str):
            return {"texto": data} # Salva como objeto JSON
        return data

class ProfissionalForm(forms.ModelForm):
    class Meta:
        model = Profissional
        exclude = ['organizacao']
        widgets = {
            'nome': forms.TextInput(attrs={'class': 'form-control'}),
            'cpf': forms.TextInput(attrs={'class': 'form-control'}),
            'crefito': forms.TextInput(attrs={'class': 'form-control'}),
            # Input de cor nativo do HTML5
            'cor_agenda': forms.TextInput(attrs={'type': 'color', 'class': 'form-control form-control-color', 'title': 'Escolha a cor da agenda'}),
            'valor_hora_aula': forms.NumberInput(attrs={'class': 'form-control', 'step': '0.01'}),
        }

class UnidadeForm(forms.ModelForm):
    class Meta:
        model = Unidade
        exclude = ['organizacao']
        widgets = {
            'nome': forms.TextInput(attrs={'class': 'form-control'}),
            'endereco': forms.TextInput(attrs={'class': 'form-control'}),
            'telefone': forms.TextInput(attrs={'class': 'form-control'}),
        }

class DocumentoExtraForm(forms.ModelForm):
    class Meta:
        model = DocumentoAluno
        fields = ['titulo', 'arquivo']
        widgets = {
            'titulo': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Ex: Laudo Médico 2024'}),
            'arquivo': forms.FileInput(attrs={'class': 'form-control'}),
        }