import os
import sys
import json
import time
import requests
import pyodbc
from settings import db_host, db_password, db_port, db_user, db_driver, db_name, api_key, api_token, enviroment

# urls from vtex api
headers = {
    'content-type': "application/json",
    'accept': "application/vnd.vtex.ds.v10+json",
    'x-vtex-api-appkey': api_key,
    'x-vtex-api-apptoken': api_token
}


start_time = time.time()
print('(+) Database connection started')
# database connection
connection = pyodbc.connect(
    "Driver={"+db_driver+"};"
    "Database="+db_name + ";"
    "Server= "+db_host+","+db_port+";"
    "UID="+db_user+";"
    "PWD="+db_password+";"
)
print('(+) Database connection ended')

query_invoice = """
SELECT
top 1
serie_document as serie, numar_document as number, data_document as date_document,
data_lim as date_limit, id_client, cif_client, den_client as name, valoare as value, valoare_cu_tva as value_tva, valoare_incasata as value_collected
FROM accesex_facturi_clienti afc
WHERE
anluna >= '202001' AND
anulare != '1' AND
validare = '1'
;"""


query_master_data = ''

cursor = connection.cursor()
cursor.execute(query_invoice)


invoices = cursor.fetchall()

# for invoice in invoices:
#     payload = {}


# url_vtex = "https://vetrob2c.vtexcommercestable.com.br/api/dataentities/CL/scroll?{}"

# response = requests.get(url_vtex.format('_size=1000&_fields=email,companyCIF,userId,id'), headers=headers)
# clients = response.json()

# if response.status_code == 400:
#     print('Don´t Exists X-VTEX-MD-TOKEN')
#     exit()
# token_md = response.headers["X-VTEX-MD-TOKEN"]

# ban = True
# while ban:
#     response = requests.get(url_vtex.format(
#         "_token={}".format(token_md)), headers=headers)
#     if response.status_code == 400:
#         ban = False
#         continue
#     clients = clients + response.json()

# {
#     "document": "99999999999",
#     "documentType": "CNPJ",
#     "email": "email@domain.com",
#     "creditLimit": "500",
#     "tolerance": "1"
# }
