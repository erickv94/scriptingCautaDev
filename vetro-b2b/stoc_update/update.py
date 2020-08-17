from settings import db_host, db_password, db_port, db_user, db_driver, api_key, api_token
import pyodbc
import requests
import time
import json
# urls from vtex api
headers = {
    'content-type': "application/json",
    'accept': "application/vnd.vtex.ds.v10+json",
    'x-vtex-api-appkey': api_key,
    'x-vtex-api-apptoken': api_token
}

url_sku_by_refid = "https://vetrob2c.vtexcommercestable.com.br/api/catalog_system/pub/sku/stockkeepingunitidsbyrefids"


start_time = time.time()
print('(+) Database connection started')
# database connection
connection = pyodbc.connect(
    "Driver={"+db_driver+"};"
    "Server= "+db_host+","+db_port+";"
    "UID="+db_user+";"
    "PWD="+db_password+";"
)
print('(+) Database connection ended')

print('(+) Query to database started')
cursor = connection.cursor()
cursor.execute("""SELECT [id_produs], SUM([stoc]) - SUM([rezervat]) AS diff_stoc_reserved 
FROM [S.C. VETRO SOLUTIONS S.R.L.].[dbo].[accesex_stoc_view] WHERE [id_gestiune] = '1(1)' AND [stoc] >= 0 
GROUP BY [id_produs];""")

stock_available = {}
refIds = []
for row in cursor:
    refId = row[0]
    stock = row[1]
    refIds.append(refId)
    stock_available[refId] = int(stock)


print('(+) Query to database ended')

print('(+) Verifying skus on nexus which exists on Vtex')
payload = "{}".format(refIds)
response = requests.request(
    "POST", url_sku_by_refid, data=payload, headers=headers)
print(refIds)
skus_available = response.json()

skus_filtered = {}

for key, value in skus_available.items():
    if value:
        skus_filtered[value] = stock_available[key]

print('(+) Verifying done on Vtex')

print('(+) Starting update invetories on Vtex')
warehouse = "1_1"

for key, value in skus_filtered.items():
    sku_id = key
    url_update_invetory = "https://vetrob2c.vtexcommercestable.com.br/api/logistics/pvt/inventory/skus/{}/warehouses/{}".format(
        sku_id, warehouse)
    payload = {"unlimitedQuantity": False,
               "quantity": value, "dateUtcOnBalanceSystem": None}
    payload = json.dumps(payload)
    response = requests.request(
        "PUT", url_update_invetory, data=payload, headers=headers)
    print(" -- SkuId: {} updated with {} units on stock, url used {}".format(key,
                                                                             value, url_update_invetory))


print('(+) Invetories updated sucessfully')
end_time = time.time()
print('Task ended at {} seconds'.format(end_time-start_time))
