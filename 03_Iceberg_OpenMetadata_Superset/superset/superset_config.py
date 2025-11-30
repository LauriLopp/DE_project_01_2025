import os

ROW_LIMIT = 5000
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "supersecretkey")
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_SQLALCHEMY_DATABASE_URI",
    "postgresql://superset:superset@superset-db:5432/superset"
)

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True
# Add endpoints that need to be exempt from CSRF protection
WTF_CSRF_EXEMPT_LIST = []
# A CSRF token that expires in 1 year
WTF_CSRF_TIME_LIMIT = 60 * 60 * 24 * 365

# Set this API key to enable Mapbox visualizations
MAPBOX_API_KEY = ""

# Enable feature flags as needed
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    # Keep legacy chart UI which includes advanced analytics section
    "DISABLE_LEGACY_DATASOURCE_EDITOR": False,
}

# Default cache configuration
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": "superset-redis",
    "CACHE_REDIS_PORT": 6379,
    "CACHE_REDIS_DB": 1,
    "CACHE_REDIS_URL": "redis://superset-redis:6379/1",
}

# Celery configuration (optional, for async queries)
class CeleryConfig:
    broker_url = "redis://superset-redis:6379/0"
    imports = ("superset.sql_lab",)
    result_backend = "redis://superset-redis:6379/0"
    worker_prefetch_multiplier = 10
    task_acks_late = True

CELERY_CONFIG = CeleryConfig

# Allow embedding dashboards in iframes
# PUBLIC_ROLE_LIKE = "Gamma"
# SESSION_COOKIE_SAMESITE = None
# SESSION_COOKIE_SECURE = False
# SESSION_COOKIE_HTTPONLY = False

# Upload folder for CSV files
UPLOAD_FOLDER = "/app/pythonpath/uploads/"
