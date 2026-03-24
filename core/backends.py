from django.contrib.auth.backends import ModelBackend
from django.contrib.auth import get_user_model
from django.db import connection

class DebugLoginBackend(ModelBackend):
    def authenticate(self, request, username=None, password=None, **kwargs):
        print(f"\n🛑 [DEBUG TERMINAL] Tentativa de Login: '{username}'")
        print(f"   -> Schema Atual do Banco: {connection.schema_name}")
        
        User = get_user_model()
        
        # 1. Tenta achar o usuário
        try:
            user = User.objects.get(username=username)
            print(f"   -> ✅ Usuário encontrado! ID: {user.id} | Org: {getattr(user, 'organizacao', 'Nenhuma')}")
        except User.DoesNotExist:
            print(f"   -> ❌ ERRO FATAL: Usuário '{username}' NÃO EXISTE no schema '{connection.schema_name}'.")
            # Tenta ver se existe no public só pra avisar
            if connection.schema_name != 'public':
                print("   -> DICA: O usuário pode estar no 'public', mas o Django não está achando.")
            return None

        # 2. Testa a Senha
        if user.check_password(password):
            print(f"   -> ✅ Senha CORRETA.")
        else:
            print(f"   -> ❌ ERRO: Senha INCORRETA.")
            return None

        # 3. Testa permissões do Django (is_active)
        if self.user_can_authenticate(user):
            print(f"   -> ✅ Usuário Ativo e pronto para logar.")
            return user
        else:
            print(f"   -> ❌ ERRO: Usuário Inativo (is_active=False).")
            return None