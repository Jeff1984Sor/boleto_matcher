from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from core.views import debug_auth
from django.contrib.auth import views as auth_views

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('core.urls')),       
    path('cadastros/', include('cadastros_fit.urls')), 
    path('agenda/', include('agenda_fit.urls')),
    path('financeiro/', include('financeiro_fit.urls')),
    path('contratos/', include('contratos_fit.urls')),
    path('comunicacao/', include('comunicacao_fit.urls')), 
    path('termos/', include('termos_fit.urls')),
    path('tools/pdf/', include('pdf_tools.urls')),
    path('app/', include('portal_aluno.urls')), 
    path('logout/', auth_views.LogoutView.as_view(), name='logout'),
    path("__reload__/", include("django_browser_reload.urls")),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)