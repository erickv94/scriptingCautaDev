# settings.py
from dotenv import load_dotenv
from pathlib import Path
import os

env_path = Path(__file__).parent.parent.absolute() / '.env'
load_dotenv(dotenv_path=env_path)

# mssql
db_name = os.getenv('DB_MASTER')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_driver = os.getenv('DB_DRIVER')

# pgsql
pg_name = os.getenv('PG_DB_NAME')
pg_user = os.getenv('PG_DB_USER')
pg_host = os.getenv('PG_DB_HOST')
pg_port = os.getenv('PG_DB_PORT')
pg_password = os.getenv('PG_DB_PASSWORD')
