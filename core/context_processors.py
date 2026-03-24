def permissoes_produtos(request):
    """
    Retorna uma lista de slugs dos produtos que a organização do usuário contratou.
    Exemplo de retorno: {'perms_produtos': ['suite_pilates', 'financeiro']}
    """
    if not request.user.is_authenticated:
        return {'perms_produtos': []}

    # Se for superusuário, libera tudo (opcional, mas bom para dev)
    if request.user.is_superuser:
        # Retorna uma lista "fictícia" com tudo, ou trata no template
        # Aqui vou retornar vazia e tratar o is_superuser no template
        return {'perms_produtos': []}

    try:
        # Verifica se tem organização vinculada
        if hasattr(request.user, 'organizacao') and request.user.organizacao:
            # Pega os slugs dos produtos (lembre que mudamos o nome para produtos_contratados)
            slugs = list(request.user.organizacao.produtos_contratados.values_list('slug', flat=True))
            return {'perms_produtos': slugs}
    except Exception:
        pass

    return {'perms_produtos': []}