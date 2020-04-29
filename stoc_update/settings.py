# settings.py
from dotenv import load_dotenv
from pathlib import Path 
import os

env_path = Path(__file__).parent.parent.absolute() / '.env'
load_dotenv(dotenv_path=env_path)

db_name=os.getenv('DB_NAME')
db_user=os.getenv('DB_USER')
db_password=os.getenv('DB_PASSWORD')
db_host=os.getenv('DB_HOST')
db_port=os.getenv('DB_PORT')
db_driver=os.getenv('DB_DRIVER')
