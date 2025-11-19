from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from .models import CustomUser,Produto,HistoricoConsumo, BannerHome

# Para gerenciar os produtos
@admin.register(Produto)
class ProdutoAdmin(admin.ModelAdmin):
    list_display = ['nome', 'slug']
    prepopulated_fields = {'slug': ('nome',)} # Preenche o slug automaticamente ao digitar o nome

class HistoricoInline(admin.TabularInline):
    model = HistoricoConsumo
    readonly_fields = ['data_fechamento', 'paginas_no_ciclo']
    extra = 0
    can_delete = False

class CustomUserAdmin(UserAdmin):
    model = CustomUser
    list_display = ['username', 'email', 'paginas_processadas', 'is_assinante']
    
    fieldsets = UserAdmin.fieldsets + (
        ('Assinatura e Métricas', {
            'fields': ('is_assinante', 'produtos', 'paginas_processadas', 'telefone', 'cpf', 'nome_empresa')
        }),
    )
    filter_horizontal = ('produtos',)
    
    # ADICIONE ESTA LINHA:
    inlines = [HistoricoInline] # <--- Isso coloca o histórico dentro do perfil


admin.site.register(CustomUser, CustomUserAdmin)

@admin.register(BannerHome)
class BannerAdmin(admin.ModelAdmin):
    list_display = ['titulo', 'ativo', 'ordem']
    list_editable = ['ativo', 'ordem']