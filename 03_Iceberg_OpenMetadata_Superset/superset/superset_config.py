import os

# ---------------------------------------------------------
# Superset Config
# ---------------------------------------------------------

# Retrieve the DB URI from the Docker environment variable
SQLALCHEMY_DATABASE_URI = os.getenv("SUPERSET_SQLALCHEMY_DATABASE_URI")

# Fallback: If the Env Var is missing, warn us (checks logs)
if not SQLALCHEMY_DATABASE_URI:
    print("WARNING: SUPERSET_SQLALCHEMY_DATABASE_URI is missing! Using SQLite.")
    # This would mean data loss on restart
    SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset.db'

# Security settings
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY")

# Feature flags (optional, but good to have)
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# Talisman (Security headers) - Disable strictly for local dev if needed, 
# but usually safe to leave default. 
# TALISMAN_ENABLED = False