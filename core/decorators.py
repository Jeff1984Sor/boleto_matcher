from django.shortcuts import redirect
from django.contrib import messages
from functools import wraps

def possui_produto(slug_produto):
    def decorator(view_func):
        @wraps(view_func)
        def _wrapped_view(request, *args, **kwargs):
            if not request.user.is_authenticated:
                return redirect('login')
            
            if request.user.is_superuser:
                return view_func(request, *args, **kwargs)
            
            # --- NOVA LÓGICA ---
            # Verifica se o usuário tem organização E se a organização tem o produto
            if request.user.organizacao and request.user.organizacao.produtos.filter(slug=slug_produto).exists():
                return view_func(request, *args, **kwargs)
            
            messages.error(request, "Sua organização não contratou este produto.")
            return redirect('home')
            
        return _wrapped_view
    return decorator