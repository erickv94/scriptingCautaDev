from settings import db_host, db_name, db_password, db_port, db_user, db_driver, api_key, api_token
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

url_sku_by_refid = "https://vetro.vtexcommercestable.com.br/api/catalog_system/pub/sku/stockkeepingunitidsbyrefids"
create_specification_url = 'https://vetro.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit/{}/specification'

print('(+) Database connection started')
# database connection
connection = pyodbc.connect(
    "Driver={"+db_driver+"};"
    "Server= "+db_host+","+db_port+";"
    "Database="+db_name + ";"
    "UID="+db_user+";"
    "PWD="+db_password+";"
)
print('(+) Database connection ended')

print('(+) Query to database started')
cursor = connection.cursor()

# query prescription products
cursor.execute("""SELECT attr_view.denumire , CONCAT(val_attr.id_pr, '(',val_attr.pl_pr,')') AS product_id, val_attr.valoare
FROM nomen3_valori_atribute val_attr
LEFT JOIN nomen3_atribute_view attr_view ON attr_view.id = val_attr.id_atribut
WHERE
attr_view.id = '72'""")

products = cursor.fetchall()

print('(+) Query to database ended')

# https://vetro.vtexcommercestable.com.br/api/catalog/pvt/stockkeepingunit?refId=

ref_ids = []
skus_available = {}
prescriptions = {}

for row in products:

    prescriptions[row[1]] = True if row[2] == 'DA' else False

    # append into ref_ids
    ref_ids.append(row[1])

# removing duplicated ref_ids
ref_ids = list(set(ref_ids))

print('(+) getting sku id through ref id')
payload = '{}'.format(ref_ids)
response = requests.request(
    "POST", url_sku_by_refid, data=payload, headers=headers)

skus_available = response.json()

# removing None values on dict
for key, value in dict(skus_available).items():
    if value is None:
        del skus_available[key]


print('(+) updating field values for skus available')
# request to update data
for key, value in skus_available.items():
    fieldValueId = 554 if prescriptions[key] else 553
    # creating payload
    payload = {}
    payload['FieldId'] = 49
    payload['FieldValueId'] = fieldValueId
    payload = json.dumps(payload)

    response = requests.request(
        "POST", create_specification_url.format(value), data=payload, headers=headers)

    if response.status_code == 409:
        print(
            '(+) skuId: {} already updated with field value:{}'.format(value, fieldValueId))
    else:
        print('(+) skuId {} updated with field value: {}'.format(value, fieldValueId))

print('(+) script sucessfully done')
