from settings import db_host, db_password, db_port, db_user, db_driver, db_name, api_key, api_token
import pyodbc
import requests
import time
import json

# urls from vtex api
headers = {
    'content-type': "application/json",
    'x-vtex-api-appkey': api_key,
    'x-vtex-api-apptoken': api_token
}


url_sku_by_refid = "https://vetrob2c.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit?refId={}"

url_update_price = "https://api.vtex.com/vetrob2c/pricing/prices/{}"


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
SELECT 
CONCAT(id_pr,'(',pl_pr,')') AS prod_id,
puv AS sale_price_no_vat,
denpr AS product_name
FROM preturi_ma_linii_view
WHERE
id_ant = '2';
""")


rows = cursor.fetchall()

products_db = []

for row in rows:
    response = requests.request(
        "GET", url_sku_by_refid.format(row[0]), headers=headers)

    if response.status_code == 404:
        continue

    sku_api = response.json()
    sku_id = sku_api['Id']

    price_decimal = str(row[1])
    price_without_decimal = price_decimal.replace('Decimal', '')
    price = float(price_without_decimal)
    sku_name = row[2]
    payload = {
        "basePrice": price,
        "markup": 0
    }
    # payload = json.dumps(payload)
    url = url_update_price.format(sku_id)
    print(url)
    response = requests.request("PUT", url, json=payload, headers=headers)

    print(response.status_code, sku_id, row[2])
    print(payload)
    print(response.text)

print('(+) Query to database ended')


end_time = time.time()

print('Task ended at {} seconds'.format(end_time-start_time))
