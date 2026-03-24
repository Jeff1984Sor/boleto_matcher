from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from .models import CustomUser, Produto, HistoricoConsumo, BannerHome, Organizacao

# --- ORGANIZAÇÃO ---
@admin.register(Organizacao)
class OrganizacaoAdmin(admin.ModelAdmin):
    list_display = ['nome', 'cnpj']
    filter_horizontal = ('produtos_contratados',) # Aqui sim, pois produtos é da organização

# --- PRODUTOS ---
@admin.register(Produto)
class ProdutoAdmin(admin.ModelAdmin):
    list_display = ['nome', 'slug']
    prepopulated_fields = {'slug': ('nome',)}

# --- BANNERS ---
@admin.register(BannerHome)
class BannerAdmin(admin.ModelAdmin):
    list_display = ['titulo', 'ativo', 'ordem']
    list_editable = ['ativo', 'ordem']

# --- USUÁRIOS ---
class HistoricoInline(admin.TabularInline):
    model = HistoricoConsumo
    readonly_fields = ['data_fechamento', 'paginas_no_ciclo']
    extra = 0
    can_delete = False

class CustomUserAdmin(UserAdmin):
    model = CustomUser
    # Mostra a organização na lista
    list_display = ['username', 'email', 'organizacao', 'paginas_processadas', 'is_assinante']
    
    fieldsets = UserAdmin.fieldsets + (
        ('Mayacorp Corp', {
            # Removemos 'produtos' e 'nome_empresa', adicionamos 'organizacao'
            'fields': ('organizacao', 'telefone', 'cpf', 'paginas_processadas', 'is_assinante')
        }),
    )
    
    inlines = [HistoricoInline]

admin.site.register(CustomUser, CustomUserAdmin)
