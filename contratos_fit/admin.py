from django.contrib import admin
from django.db import transaction
from .models import TemplateContrato, Plano, Contrato, HorarioFixo
from .forms import HorarioFixoFormSet
# Importamos o serviço mestre que criamos anteriormente
from .services import processar_novo_contrato 

class HorarioFixoInline(admin.TabularInline):
    model = HorarioFixo
    formset = HorarioFixoFormSet # Aplica a validação de quantidade (Ex: max 2x semana)
    extra = 1

@admin.register(Contrato)
class ContratoAdmin(admin.ModelAdmin):
    list_display = ['id', 'aluno', 'plano', 'data_inicio', 'data_fim', 'valor_total', 'status']
    list_filter = ['status', 'unidade', 'plano']
    search_fields = ['aluno__nome']
    inlines = [HorarioFixoInline]
    
    # Utilitário para salvar automaticamente o usuário logado ou unidade se precisasse
    # def save_model(self, request, obj, form, change): ...

    def save_related(self, request, form, formsets, change):
        """
        Este método é chamado APÓS salvar o contrato pai e os horários filhos (inlines).
        É o momento perfeito para disparar a automação.
        """
        super().save_related(request, form, formsets, change)

        # Se for uma EDIÇÃO (change=True), evitamos rodar a automação para não duplicar parcelas/aulas.
        # Rodamos apenas na CRIAÇÃO (change=False).
        if not change:
            contrato = form.instance

            def disparar_automacao():
                try:
                    processar_novo_contrato(contrato)
                    self.message_user(request, "✅ Agenda e Financeiro gerados com sucesso!")
                except Exception as e:
                    self.message_user(request, f"⚠️ Contrato salvo, mas houve erro na automação: {e}", level='ERROR')
            
            # transaction.on_commit garante que só roda se o banco de dados confirmar que salvou tudo sem erros
            transaction.on_commit(disparar_automacao)

@admin.register(Plano)
class PlanoAdmin(admin.ModelAdmin):
    list_display = ['nome', 'valor_mensal', 'frequencia_semanal', 'duracao_meses']
    

@admin.register(TemplateContrato)
class TemplateAdmin(admin.ModelAdmin):
    list_display = ['nome', 'organizacao', 'ativo']