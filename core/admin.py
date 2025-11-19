from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from .models import CustomUser

class CustomUserAdmin(UserAdmin):
    model = CustomUser
    
    # Adiciona os campos personalizados na lista de visualização (colunas da tabela)
    list_display = ['username', 'email', 'telefone', 'nome_empresa', 'is_assinante', 'is_staff']
    
    # Adiciona os campos na tela de edição do usuário
    fieldsets = UserAdmin.fieldsets + (
        ('Informações Mayacorp', {'fields': ('telefone', 'cpf', 'nome_empresa', 'is_assinante', 'data_expiracao')}),
    )

# Registra o modelo
admin.site.register(CustomUser, CustomUserAdmin)