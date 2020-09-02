from settings import db_host, db_password, db_port, db_user, db_driver, db_name, api_key, api_token
import pyodbc
import requests
import time
import json

# urls from vtex api
"""
{
    "Message": "Operation not found for this token: 'DXF1ZXJ5QW5kRmV0Y2gBAAAAAABGsYUWOW5lQ3FxLW9TTVNrQjB0U1RMdGVwQQ=='"
}
"""
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
    "Database="+db_name + ";"
    "Driver={"+db_driver+"};"
    "Server= "+db_host+","+db_port+";"
    "UID="+db_user+";"
    "PWD="+db_password+";"
)
print('(+) Database connection ended')

print('(+) Query to database started')

cursor = connection.cursor()

cursor.execute("""
SELECT TOP 3 id_intern as id, denumire as name, email, 
registru_comert as register,cif_cnp as cif, 
termen_incasare as collection_term,  termen_plata as payment_term , 
limita_credit as credit, restanta_max as  rest
FROM accesex_parteneri_view WHERE id_agent != '0(0)';
""")


rows = cursor.fetchall()
"""
for row in rows:
    print(row.email)
"""
end_time = time.time()

clients = []

url_vtex_scroll = "https://vetrob2c.vtexcommercestable.com.br/api/dataentities/CL/scroll?_size=1000{}"

response = requests.get(url_vtex_scroll, headers=headers)
clients = response.json()

if response.status_code == 400:
    print('Don´t Exists X-VTEX-MD-TOKEN')
    exit()
token_md = response.headers["X-VTEX-MD-TOKEN"]

ban = True
while ban:
    response = requests.get(url_vtex_scroll.format(
        "&_token={}".format(token_md)), headers=headers)
    if response.status_code == 400:
        ban = False
        continue
    clients = clients + response.json()

print(clients)
print(len(clients))

print('Task ended at {} seconds'.format(end_time-start_time))
