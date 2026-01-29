"""
Django settings for mayacorp project.
"""

from pathlib import Path
import os
try:
    import dj_database_url
except ImportError:
    dj_database_url = None
from dotenv import load_dotenv
import google.generativeai as genai

# Carrega variáveis de ambiente (.env)
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.getenv('SECRET_KEY', 'django-insecure-chave-padrao-dev')
DEBUG = os.getenv('DEBUG', 'False') == 'True'

ALLOWED_HOSTS = [
    '34.171.206.16', 
    'mayacorp.com.br', 
    'www.mayacorp.com.br', 
    'localhost', 
    '127.0.0.1',
    '.localhost', 
    '.railway.app', 
    '.onrender.com', 
    '*' 
]

CSRF_TRUSTED_ORIGINS = [
    'https://*.railway.app', 
    'https://*.onrender.com',
    'https://*.mayacorp.com.br'
]

# ==============================================================================
# CONFIGURAÇÃO MULTI-TENANT (DJANGO-TENANTS)
# ==============================================================================

SHARED_APPS = (
    'django_tenants',
    'core',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    
    'crispy_forms',
    'crispy_bootstrap5',
)

TENANT_APPS = (
    'pdf_tools',
)

INSTALLED_APPS = list(SHARED_APPS) + [app for app in TENANT_APPS if app not in SHARED_APPS]

TENANT_MODEL = "core.Organizacao" 
TENANT_DOMAIN_MODEL = "core.Domain"

# ==============================================================================
# MIDDLEWARE
# ==============================================================================

MIDDLEWARE = [
    'django_tenants.middleware.main.TenantMainMiddleware', 
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    
]

ROOT_URLCONF = 'mayacorp.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / 'templates'],
        'APP_DIRS': True, 
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.request', 
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
                'core.context_processors.permissoes_produtos', 
            ],
        },
    },
]

WSGI_APPLICATION = 'mayacorp.wsgi.application'

# ==============================================================================
# DATABASE & ROUTER
# ==============================================================================

DATABASE_ROUTERS = (
    'django_tenants.routers.TenantSyncRouter',
)

DATABASES = {
    'default': {
        'ENGINE': 'django_tenants.postgresql_backend', 
        'NAME': os.getenv('DB_NAME', 'mayacorp_db'),
        'USER': os.getenv('DB_USER', 'postgres'),
        'PASSWORD': os.getenv('DB_PASSWORD', 'postgres'),
        'HOST': os.getenv('DB_HOST', 'localhost'),
        'PORT': os.getenv('DB_PORT', '5432'),
    }
}

if os.getenv('DATABASE_URL') and dj_database_url:
    db_config = dj_database_url.config(default=os.getenv('DATABASE_URL'))
    db_config['ENGINE'] = 'django_tenants.postgresql_backend'
    DATABASES = {'default': db_config}

# ==============================================================================
# CONFIGURAÇÃO DE ESTÁTICOS E MÍDIA
# ==============================================================================

STATIC_URL = '/static/'
STATIC_ROOT = BASE_DIR / 'staticfiles'

STATICFILES_DIRS = []

# Configuração de Storage (Django 4.2+)
STORAGES = {
    "default": {
        "BACKEND": "django_tenants.files.storages.TenantFileSystemStorage",
    },
}

MEDIA_URL = '/media/'
MEDIA_ROOT = BASE_DIR / 'media'
MULTITENANT_RELATIVE_MEDIA_ROOT = "%s"


# ==============================================================================
# OUTRAS CONFIGURAÇÕES (AUTH, CRISPY, GOOGLE)
# ==============================================================================

AUTH_USER_MODEL = 'core.CustomUser'
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
LANGUAGE_CODE = 'pt-br'
TIME_ZONE = 'America/Sao_Paulo'
USE_I18N = True
USE_TZ = True

CRISPY_ALLOWED_TEMPLATE_PACKS = "bootstrap5"
CRISPY_TEMPLATE_PACK = "bootstrap5"

GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
if GOOGLE_API_KEY:
    genai.configure(api_key=GOOGLE_API_KEY)

# Segurança de Cookies
if not DEBUG:
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
    SECURE_SSL_REDIRECT = True

LOGIN_URL = 'login'
LOGIN_REDIRECT_URL = 'dashboard'
LOGOUT_REDIRECT_URL = 'login'
