from dotenv import load_dotenv
import os
from pathlib import Path


env_path = Path(__file__).parent.parent.absolute() / '.env'
load_dotenv(env_path)

api_key = os.getenv('API_KEY')
api_token = os.getenv('API_TOKEN')
account_vtex = os.getenv('ACCOUNT')

db_name = os.getenv('DB_MASTER')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_driver = os.getenv('DB_DRIVER')
