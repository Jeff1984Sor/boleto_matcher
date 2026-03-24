from django.core.management.base import BaseCommand
from core.models import CustomUser, HistoricoConsumo
from django.utils import timezone

class Command(BaseCommand):
    help = 'Fecha o ciclo mensal: Salva histórico e zera contadores'

    def handle(self, *args, **kwargs):
        # Filtra só quem usou alguma coisa (maior que 0)
        usuarios_ativos = CustomUser.objects.filter(paginas_processadas__gt=0)
        
        count = 0
        for user in usuarios_ativos:
            # 1. Salva no histórico
            HistoricoConsumo.objects.create(
                usuario=user,
                paginas_no_ciclo=user.paginas_processadas,
                data_fechamento=timezone.now().date()
            )
            
            # 2. Guarda o valor antigo pra log
            valor_antigo = user.paginas_processadas
            
            # 3. Zera o contador
            user.paginas_processadas = 0
            user.save()
            
            count += 1
            self.stdout.write(f"Fechado: {user.username} ({valor_antigo} pgs)")

        self.stdout.write(self.style.SUCCESS(f'Ciclo fechado com sucesso! {count} usuários processados.'))