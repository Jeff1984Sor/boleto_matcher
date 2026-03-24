from django import forms
from django.contrib.auth.forms import UserCreationForm
from .models import CustomUser

class CustomUserCreationForm(UserCreationForm):
    class Meta:
        model = CustomUser
        fields = ('username', 'email') # Campos que vão aparecer no cadastro

class UsuarioSistemaForm(forms.ModelForm):
    password = forms.CharField(widget=forms.PasswordInput, label="Senha")
    
    class Meta:
        model = CustomUser
        # Campos que o dono do studio vai preencher
        fields = ['username', 'first_name', 'last_name', 'email', 'telefone', 'password', 'is_active']
        
    def save(self, commit=True):
        # Lógica para criptografar a senha corretamente
        user = super().save(commit=False)
        user.set_password(self.cleaned_data["password"])
        if commit:
            user.save()
        return user