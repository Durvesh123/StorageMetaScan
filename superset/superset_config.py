import os

# A strong random secret key (you can generate with: openssl rand -base64 42)
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "replace-me-with-secure-random")

# SQLAlchemy connection string for the metadata database (using SQLite for now)
SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://root:<password>@localhost/storage_metadata'

# Enable CORS (useful if you’re connecting from other tools)
ENABLE_CORS = True

# Flask app settings
APP_NAME = 'Apache Superset'

# Optional — allows all origins (for now)
CORS_OPTIONS = {
    'supports_credentials': True,
    'allow_headers': ['*'],
    'resources': ['*'],
    'origins': ['*']
}

