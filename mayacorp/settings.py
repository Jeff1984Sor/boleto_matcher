"""
Django settings for mayacorp project.
"""

from pathlib import Path
import os
from dotenv import load_dotenv
import google.generativeai as genai

# Carrega variáveis de ambiente
load_dotenv()

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.getenv('SECRET_KEY', 'django-insecure-chave-padrao-dev')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = os.getenv('DEBUG') == 'True'

ALLOWED_HOSTS = [
    '34.171.206.16', 
    'mayacorp.com.br', 
    'www.mayacorp.com.br', 
    'localhost', 
    '127.0.0.1',
    '.localhost', # Necessário para subdominios locais (ex: padaria.localhost)
    '*'
]


# ==============================================================================
# CONFIGURAÇÃO MULTI-TENANT (DJANGO-TENANTS)
# ==============================================================================

# 1. Apps Compartilhados (SHARED) - Existem no Schema Public
# Aqui ficam: Admin, Auth, Sessões e o App que controla quem são os clientes
SHARED_APPS = (
    'django_tenants',  # Obrigatório ser o primeiro
    'core',        # APP NOVO: Onde ficam os models Organizacao e Domain
    
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',

    # Libs visuais podem ser compartilhadas
    'crispy_forms',
    'crispy_bootstrap5',
)

# 2. Apps do Inquilino (TENANT) - Existem isolados em cada Schema
# Aqui ficam: Seus apps de negócio e o Usuário (para isolar login)
TENANT_APPS = (
    #'core', # Onde está o CustomUser (Isolado por empresa)
    
    # Seus Apps de Negócio
    'pdf_tools',
    'cadastros_fit',
    'contratos_fit',
    'agenda_fit',
    'financeiro_fit',
    'comunicacao_fit',
    'portal_aluno',
)

# O Django precisa da lista completa em INSTALLED_APPS
INSTALLED_APPS = list(SHARED_APPS) + [app for app in TENANT_APPS if app not in SHARED_APPS]

# 3. Modelos que definem o Tenant
TENANT_MODEL = "core.Organizacao" 
TENANT_DOMAIN_MODEL = "core.Domain"


# ==============================================================================
# MIDDLEWARE
# ==============================================================================

MIDDLEWARE = [
    'django_tenants.middleware.main.TenantMainMiddleware', # <--- OBRIGATÓRIO SER O PRIMEIRO
    
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
        'APP_DIRS': True, # O Django vai procurar templates dentro dos apps tenant automaticamente
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.request', # Obrigatório para o django-tenants
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

# Banco de Dados
# ATENÇÃO: django-tenants EXIGE PostgreSQL. SQLite não funciona com schemas.
DATABASES = {
    'default': {
        'ENGINE': 'django_tenants.postgresql_backend', # OBRIGATÓRIO SER ESSE
        'NAME': os.getenv('DB_NAME'),
        'USER': os.getenv('DB_USER'),
        'PASSWORD': os.getenv('DB_PASSWORD'),
        'HOST': os.getenv('DB_HOST'),
        'PORT': os.getenv('DB_PORT'),
    }
}

# ============================================================
# CACHE CONFIGURATION
# ============================================================

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'unique-snowflake',
        'TIMEOUT': 3600 * 24,  # 24 horas
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


# Static files (CSS, JavaScript, Images)
STATIC_URL = 'static/'
STATICFILES_DIRS = [ BASE_DIR / "static", ]
STATIC_ROOT = BASE_DIR / 'static_root'

# Configuração de Arquivos de Mídia (Uploads)
# IMPORTANTE: O django-tenants separa uploads por pasta do schema automaticamente se configurado no FileStorage,
# mas por padrão vai tudo para a mesma pasta.
MEDIA_URL = '/media/'
MEDIA_ROOT = BASE_DIR / 'media'


# Default primary key field type
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# --- MINHAS CONFIGURAÇÕES ---

# Usuário Personalizado
# Como 'core' está em TENANT_APPS, cada schema terá sua própria tabela de usuários.
AUTH_USER_MODEL = 'core.CustomUser'

# Redirecionamento de Login/Logout
LOGIN_REDIRECT_URL = 'home'
LOGOUT_REDIRECT_URL = 'home'
LOGIN_URL = 'login'

# Configuração do Crispy Forms (Bootstrap 5)
CRISPY_ALLOWED_TEMPLATE_PACKS = "bootstrap5"
CRISPY_TEMPLATE_PACK = "bootstrap5"

# Configuração do Gemini
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')

if GOOGLE_API_KEY:
    genai.configure(api_key=GOOGLE_API_KEY)
else:
    print("⚠️ AVISO: GOOGLE_API_KEY não encontrada no arquivo .env")


#SESSION_COOKIE_DOMAIN = '.localhost'

# 2. Nome do cookie (para evitar conflito com outros projetos)
SESSION_COOKIE_NAME = 'mayacorp_session'

# 3. Garante que o backend de autenticação é o padrão (sem invenções)
AUTHENTICATION_BACKENDS = [
    'django.contrib.auth.backends.ModelBackend',
]



# 4. Relaxa a segurança apenas para desenvolvimento local
SESSION_COOKIE_SECURE = False
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = 'Lax'

STORAGES = {
    "default": {
        "BACKEND": "django_tenants.files.storages.TenantFileSystemStorage", 
    },
    "staticfiles": {
        "BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage",
    },
}

# Dica: O MULTITENANT_RELATIVE_MEDIA_ROOT diz para o Django criar pastas dentro do /media/
# Se você não colocar nada, ele cria media/nome_schema/
MULTITENANT_RELATIVE_MEDIA_ROOT = "%s"

# ============================================================
# GEMINI API KEY
# ============================================================

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', 'AIzaSyAeFFYrpTCRDP9NZPRRwoa4vC8KWE7UNVQ')
