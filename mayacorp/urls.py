from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from core.views import debug_auth

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('core.urls')),       # Tudo que for raiz vai para o Core
    path('tools/pdf/', include('pdf_tools.urls')), # Apps ficarão organizados
    path('app/', include('portal_aluno.urls')), # O aluno acessa /app/
    path('cadastros/', include('cadastros_fit.urls')), 
    path('debug-login/', debug_auth),
    path('agenda/', include('agenda_fit.urls')),
    path('financeiro/', include('financeiro_fit.urls')),
    path('contratos/', include('contratos_fit.urls')),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)