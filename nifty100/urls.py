import os
from django.core.wsgi import get_wsgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "nifty100.settings")

# 'application' for Gunicorn/uWSGI (local + Docker)
application = get_wsgi_application()

# 'app' is required by Vercel's Python runtime
app = application