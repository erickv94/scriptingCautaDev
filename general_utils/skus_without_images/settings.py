from dotenv import load_dotenv
import os
from pathlib import Path


env_path = Path(__file__).parent.parent.absolute() / '.env'
load_dotenv(env_path)

api_key = os.getenv('API_KEY')
api_token = os.getenv('API_TOKEN')
account_vtex = os.getenv('ACCOUNT')
