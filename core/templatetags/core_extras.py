from django import template

register = template.Library()

@register.filter(name='tem_acesso')
def tem_acesso(user, slug_produto):
    if not user.is_authenticated:
        return False
    if user.is_superuser:
        return True
    # Verifica na organização
    if user.organizacao and user.organizacao.produtos_contratados.filter(slug=slug_produto).exists():
        return True
    return False