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
SELECT  
apv.id_intern as id, apv.denumire as name, apv.email, 
apv.registru_comert as register,apv.cif_cnp as cif, CONCAT(pv.atr_fiscal,' ',pv.cod_fiscal) as cif_concat, 
apv.termen_incasare as collection_term,  apv.termen_plata as payment_term , 
apv.limita_credit as credit, apv.restanta_max as  rest
FROM accesex_parteneri_view  apv inner join parteneri_view pv
ON
 apv.id_intern = CONCAT(pv.id, '(',pv.pct_lcr,')')
WHERE id_agent != '0(0)';
""")


customers = cursor.fetchall()


clients = []

url_vtex_scroll = "https://vetrob2c.vtexcommercestable.com.br/api/dataentities/CL/scroll{}"

response = requests.get(url_vtex_scroll.format('?_size=1000&_fields=email,companyCIF,userId,id'), headers=headers)
clients = response.json()

if response.status_code == 400:
    print('Don´t Exists X-VTEX-MD-TOKEN')
    exit()
token_md = response.headers["X-VTEX-MD-TOKEN"]

ban = True
while ban:
    response = requests.get(url_vtex_scroll.format(
        "?_token={}".format(token_md)), headers=headers)
    if response.status_code == 400:
        ban = False
        continue
    clients = clients + response.json()

print('clients qty: {}'.format(len(clients)))
url_account_base = "https://vetrob2c.vtexcommercestable.com.br/api/creditcontrol/accounts{}"
url_update_credit_limit = 'https://vetrob2c.vtexcommercestable.com.br/api/creditcontrol/accounts/{}/creditlimit'
url_update_credit_partiality = 'https://vetrob2c.vtexcommercestable.com.br/api/creditcontrol/accounts/{}'
for client in clients:
    # don't execute if it doesnt have a cif
    if not client['companyCIF']:
        print('+ customer does not have cif: {}'.format(client['email']))
        continue

    # getting respose for checking existing accounts
    response = requests.get(url_account_base.format('?email={}'.format(client['email'])), headers=headers)
    accountFounded = response.json()['data']

    print('+ customer processed: {}'.format(client['email']))
    # filter from database result
    credit_limit_acc = None
    for customer in customers:
        if client['companyCIF'].replace(' ', '') == customer.cif_concat.replace(' ', ''):
            # if client['email'] in customer.email:
            credit_limit_acc = customer
            break

    if not credit_limit_acc:
        continue

    # if there's not an account open to the an email, it will be open
    if len(accountFounded) == 0:
        payloadCreate = {
            "document": credit_limit_acc.cif,
            "documentType": "CNPJ",
            "email": client['email'],
            "creditLimit": float(credit_limit_acc.credit),
            "tolerance": "1"
        }

        responseCreated = requests.post(url_account_base.format(''), headers=headers, json=payloadCreate)
        print('--- created account credit')
    else:
        accountCreditUpdate = accountFounded[0]['id']
        payloadValue = {
            "email": client['email'],
            'creditLimit': float(credit_limit_acc.credit),
            "document": credit_limit_acc.cif,
            "id": accountCreditUpdate
        }
        responseUpdate = requests.put(url_update_credit_partiality.format(accountCreditUpdate),
                                      headers=headers, json=payloadValue)
        print('--- update credit limit')
