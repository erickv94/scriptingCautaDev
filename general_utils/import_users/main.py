from settings import api_key, api_token, account_vtex
import requests
import pandas as pd
from pandas import ExcelFile
from pathlib import Path

headers = {
    'content-type': "application/json",
    'accept': "application/vnd.vtex.ds.v10+json",
    'x-vtex-api-appkey': api_key,
    'x-vtex-api-apptoken': api_token
}


url = "https://vetrob2c.vtexcommercestable.com.br/api/dataentities/CL/documents"

current_path = Path(__file__).parent.absolute()
skus_xls = current_path/'data.xlsx'
default_sheet = 'Teamshare Export'
df_skus = pd.read_excel(skus_xls, sheet_name=default_sheet)

for index, row in df_skus.iterrows():
    pass
# response = requests.put(url, headers=headers, data=payload)
