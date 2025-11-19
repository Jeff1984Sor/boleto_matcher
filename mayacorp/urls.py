from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('core.urls')),       # Tudo que for raiz vai para o Core
    path('tools/pdf/', include('pdf_tools.urls')), # Apps ficarão organizados
]