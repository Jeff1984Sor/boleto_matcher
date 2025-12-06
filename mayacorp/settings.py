"""
Django settings for mayacorp project.
"""

from pathlib import Path
import os
import dj_database_url # <--- BIBLIOTECA NOVA (pip install dj-database-url)
from dotenv import load_dotenv
import google.generativeai as genai

# Carrega variáveis de ambiente (.env)
load_dotenv()

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.getenv('SECRET_KEY', 'django-insecure-chave-padrao-dev')

# SECURITY WARNING: don't run with debug turned on in production!
# Se não tiver a variável DEBUG, assume False (Produção)
DEBUG = os.getenv('DEBUG', 'False') == 'True'

ALLOWED_HOSTS = [
    '34.171.206.16', 
    'mayacorp.com.br', 
    'www.mayacorp.com.br', 
    'localhost', 
    '127.0.0.1',
    '.localhost', 
    '.railway.app', # Permite subdominios no Railway
    '.onrender.com', # Permite subdominios no Render
    '*' # Cuidado em produção real, mas útil para testes iniciais
]

# Configuração CSRF para funcionar em HTTPS (Railway/Render)
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
    'cadastros_fit',
    'contratos_fit',
    'agenda_fit',
    'financeiro_fit',
    'comunicacao_fit',
    'portal_aluno',
)

INSTALLED_APPS = list(SHARED_APPS) + [app for app in TENANT_APPS if app not in SHARED_APPS]

TENANT_MODEL = "core.Organizacao" 
TENANT_DOMAIN_MODEL = "core.Domain"


# ==============================================================================
# MIDDLEWARE
# ==============================================================================

MIDDLEWARE = [
    'django_tenants.middleware.main.TenantMainMiddleware', # OBRIGATÓRIO PRIMEIRO
    
    'django.middleware.security.SecurityMiddleware',
    
    'whitenoise.middleware.WhiteNoiseMiddleware', # <--- OBRIGATÓRIO PARA ESTÁTICOS NA NUVEM
    
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
# DATABASE ROUTER
# ==============================================================================

DATABASE_ROUTERS = (
    'django_tenants.routers.TenantSyncRouter',
)

# ==============================================================================
# BANCO DE DADOS (HÍBRIDO: LOCAL E NUVEM)
# ==============================================================================

# Configuração padrão (Local) lendo variáveis separadas
DATABASES = {
    'default': {
        'ENGINE': 'django_tenants.postgresql_backend', # Engine Especial
        'NAME': os.getenv('DB_NAME', 'mayacorp_db'),
        'USER': os.getenv('DB_USER', 'postgres'),
        'PASSWORD': os.getenv('DB_PASSWORD', 'postgres'),
        'HOST': os.getenv('DB_HOST', 'localhost'),
        'PORT': os.getenv('DB_PORT', '5432'),
    }
}

# Se existir DATABASE_URL (Railway/Render fornece isso automaticamente)
if os.getenv('DATABASE_URL'):
    # Parseia a URL do banco
    db_config = dj_database_url.config(default=os.getenv('DATABASE_URL'))
    
    # IMPORTANTE: Força a engine do Tenant, pois o dj_database_url usa a padrão
    db_config['ENGINE'] = 'django_tenants.postgresql_backend'
    
    DATABASES = {'default': db_config}


# ============================================================
# CACHE CONFIGURATION
# ============================================================

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'unique-snowflake',
        'TIMEOUT': 3600 * 24, 
    }
}

# ==============================================================================
# Password validation
# ==============================================================================
AUTH_PASSWORD_VALIDATORS = [
    { 'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator', },
    { 'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator', },
    { 'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator', },
    { 'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator', },
]


# Internationalization
LANGUAGE_CODE = 'pt-br'
TIME_ZONE = 'America/Sao_Paulo'
USE_I18N = True
USE_TZ = True


# ==============================================================================
# ARQUIVOS ESTÁTICOS E MÍDIA
# ==============================================================================

STATIC_URL = 'static/'
STATICFILES_DIRS = [ BASE_DIR / "static", ]
STATIC_ROOT = BASE_DIR / 'staticfiles' # Pasta onde o collectstatic junta tudo

# Armazenamento de Estáticos com Compressão (Whitenoise)
STATICFILES_STORAGE = 'whitenoise.storage.CompressedManifestStaticFilesStorage'

# Mídia (Uploads)
MEDIA_URL = '/media/'
MEDIA_ROOT = BASE_DIR / 'media'

# Configuração de Storage do Tenant
STORAGES = {
    "default": {
        "BACKEND": "django_tenants.files.storages.TenantFileSystemStorage", 
    },
    "staticfiles": {
        "BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage",
    },
}

MULTITENANT_RELATIVE_MEDIA_ROOT = "%s"


# Default primary key field type
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# --- MINHAS CONFIGURAÇÕES ---

AUTH_USER_MODEL = 'core.CustomUser'

LOGIN_REDIRECT_URL = 'home'
LOGOUT_REDIRECT_URL = 'home'
LOGIN_URL = 'login'

CRISPY_ALLOWED_TEMPLATE_PACKS = "bootstrap5"
CRISPY_TEMPLATE_PACK = "bootstrap5"

GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
if GOOGLE_API_KEY:
    genai.configure(api_key=GOOGLE_API_KEY)


SESSION_COOKIE_NAME = 'mayacorp_session'
AUTHENTICATION_BACKENDS = [
    'django.contrib.auth.backends.ModelBackend',
]

# Segurança de Cookies (Ativa apenas se não for DEBUG, ou seja, em Produção)
if not DEBUG:
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_HTTPONLY = True
    CSRF_COOKIE_SECURE = True
    SECURE_SSL_REDIRECT = True # Força HTTPS na nuvem
else:
    SESSION_COOKIE_SECURE = False
    SESSION_COOKIE_HTTPONLY = True




GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')


# Configuração de E-mail
EMAIL_BACKEND = os.getenv('EMAIL_BACKEND', 'django.core.mail.backends.console.EmailBackend') # Default para console se não tiver config
EMAIL_HOST = os.getenv('EMAIL_HOST')
EMAIL_PORT = os.getenv('EMAIL_PORT')
EMAIL_USE_TLS = os.getenv('EMAIL_USE_TLS') == 'True'
EMAIL_HOST_USER = os.getenv('EMAIL_HOST_USER')
EMAIL_HOST_PASSWORD = os.getenv('EMAIL_HOST_PASSWORD')
DEFAULT_FROM_EMAIL = os.getenv('DEFAULT_FROM_EMAIL', 'noreply@mayacorp.com.br')