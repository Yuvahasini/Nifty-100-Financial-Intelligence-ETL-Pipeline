"""
Django settings for nifty100 project.
"""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.getenv("DJANGO_SECRET_KEY", "change-me-in-production-use-env-var")

DEBUG = os.getenv("DJANGO_DEBUG", "True") == "True"

# ---------------------------------------------------------------------------
# Allowed Hosts — automatically includes Vercel domains + any custom domains
# ---------------------------------------------------------------------------
_default_hosts = "localhost,127.0.0.1"
ALLOWED_HOSTS = os.getenv("DJANGO_ALLOWED_HOSTS", _default_hosts).split(",")

# Always allow all *.vercel.app subdomains and the Vercel deployment URLs
ALLOWED_HOSTS += [
    ".vercel.app",            # covers all *.vercel.app including preview URLs
    "nifty-100-financial-intelligence-et.vercel.app",
]

# In production on Vercel, DEBUG is False but we still need to accept the host.
# Setting ALLOWED_HOSTS = ["*"] is fine when behind Vercel's edge proxy which
# validates the host. We do this only when not in local dev.
if not DEBUG and not os.getenv("DJANGO_ALLOWED_HOSTS"):
    ALLOWED_HOSTS = ["*"]

# ---------------------------------------------------------------------------
# Applications
# ---------------------------------------------------------------------------
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    # Third-party
    "rest_framework",
    "corsheaders",
    "drf_spectacular",
    # Project
    "intelligence",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "corsheaders.middleware.CorsMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "nifty100.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "nifty100.wsgi.application"

# ---------------------------------------------------------------------------
# Database — points to the same PostgreSQL warehouse
# ---------------------------------------------------------------------------
DATABASES = {
    "default": {
        "ENGINE":   "django.db.backends.postgresql",
        "NAME":     os.getenv("DB_NAME",     "nifty100"),
        "USER":     os.getenv("DB_USER",     "postgres"),
        "PASSWORD": os.getenv("DB_PASSWORD", "postgres"),
        "HOST":     os.getenv("DB_HOST",     "localhost"),
        "PORT":     os.getenv("DB_PORT",     "5433"),
        "OPTIONS":  {"options": "-c search_path=public"},
    }
}

# ---------------------------------------------------------------------------
# Celery — background ETL refresh
# ---------------------------------------------------------------------------
CELERY_BROKER_URL        = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_RESULT_BACKEND    = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")
CELERY_TIMEZONE          = "Asia/Kolkata"
CELERY_TASK_SERIALIZER   = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_ACCEPT_CONTENT    = ["json"]

from celery.schedules import crontab
CELERY_BEAT_SCHEDULE = {
    # Re-run ML scoring every day at 6:00 AM IST
    "daily-ml-scoring": {
        "task":     "intelligence.tasks.run_ml_scoring",
        "schedule": crontab(hour=6, minute=0),
    },
}

# ---------------------------------------------------------------------------
# Django REST Framework
# ---------------------------------------------------------------------------
REST_FRAMEWORK = {
    "DEFAULT_PAGINATION_CLASS": "rest_framework.pagination.PageNumberPagination",
    "PAGE_SIZE":                 20,
    "DEFAULT_SCHEMA_CLASS":      "drf_spectacular.openapi.AutoSchema",
    "DEFAULT_RENDERER_CLASSES": [
        "rest_framework.renderers.JSONRenderer",
        "rest_framework.renderers.BrowsableAPIRenderer",
    ],
    # Throttling disabled in development — Redis not required locally.
    # Uncomment below when deploying to production with Redis running.
    # "DEFAULT_THROTTLE_CLASSES": [
    #     "rest_framework.throttling.AnonRateThrottle",
    #     "rest_framework.throttling.UserRateThrottle",
    # ],
    # "DEFAULT_THROTTLE_RATES": {
    #     "anon": "100/hour",
    #     "user": "1000/hour",
    # },
}

# ---------------------------------------------------------------------------
# drf-spectacular (Swagger / OpenAPI)
# ---------------------------------------------------------------------------
SPECTACULAR_SETTINGS = {
    "TITLE":       "Nifty 100 Financial Intelligence API",
    "DESCRIPTION": "REST API for Nifty 100 company financials, ML health scores, and analytics.",
    "VERSION":     "1.0.0",
    "SERVE_INCLUDE_SCHEMA": False,
}

# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------
CORS_ALLOWED_ORIGINS = os.getenv(
    "CORS_ALLOWED_ORIGINS",
    "http://localhost:3000,http://127.0.0.1:3000,http://localhost:5500,http://127.0.0.1:5500"
).split(",")

# Allow all origins in debug mode OR when deployed on Vercel
CORS_ALLOW_ALL_ORIGINS = DEBUG or bool(os.getenv("VERCEL", ""))

# Allow Vercel preview + production URLs explicitly
CORS_ALLOWED_ORIGIN_REGEXES = [
    r"^https://.*\.vercel\.app$",
]

# ---------------------------------------------------------------------------
# CSRF trusted origins — needed for POST requests from Vercel frontend
# ---------------------------------------------------------------------------
CSRF_TRUSTED_ORIGINS = [
    "https://*.vercel.app",
    "https://nifty-100-financial-intelligence-et.vercel.app",
    "http://localhost:5500",
    "http://127.0.0.1:5500",
]

# ---------------------------------------------------------------------------
# Static & media
# ---------------------------------------------------------------------------
STATIC_URL  = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
MEDIA_URL   = "/media/"
MEDIA_ROOT  = BASE_DIR / "media"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# ---------------------------------------------------------------------------
# Caching
# Use local memory cache in development (no Redis needed).
# Switch to RedisCache in production by setting REDIS_URL env variable.
# ---------------------------------------------------------------------------
REDIS_URL = os.getenv("REDIS_URL", "")

if REDIS_URL:
    CACHES = {
        "default": {
            "BACKEND":  "django.core.cache.backends.redis.RedisCache",
            "LOCATION": REDIS_URL,
        }
    }
else:
    CACHES = {
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        }
    }