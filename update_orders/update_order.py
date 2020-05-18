from settings import db_name, db_host, db_password, db_port, db_user, db_driver, api_key, api_token
import pyodbc
import requests
import time
import json
import hashlib


def get_data(url, id='master'):

    if id == 'master':
        url_fetch = url
    else:
        url_fetch = url+str(id)

    response = requests.request(
        "GET", url_fetch, headers=headers)

    data = response.json()
    return data


def cleaned_last_profiles(client_list):
    profiles_cleaned = {}
    profiles_cleaned_list = []

    for client in client_list:
        profiles_cleaned[client['profile_id']] = client

    for key in profiles_cleaned.keys():
        profiles_cleaned_list.append(profiles_cleaned[key])

    return profiles_cleaned_list


def parseToFloatVtex(value):
    order_total_str = str(value)
    if '.' in order_total_str:
        order_total_str = order_total_str.split('.')[0]

    order_total_float = order_total_str[:-2] + '.' + order_total_str[-2:]
    return float(order_total_float)


def destructuring_order(order_info, order):
    profile_id = order_info['clientProfileData']['userProfileId']
    order_data = {}
    order_data['profile_id'] = profile_id
    order_data['order_id'] = order_info['orderId']
    print(order_data['order_id'])
    order_data['creation_date'] = order_info['creationDate']
    order_data['estimation_date'] = order['ShippingEstimatedDateMax']
    order_data['total'] = parseToFloatVtex(
        order['totalValue'])  # needs to parse
    payment_data = order_info['paymentData']
    payment_observation = ''

    for transactions in payment_data['transactions']:
        for payment in transactions['payments']:
            payment_observation = payment_observation + \
                "paymentSystemName: " + payment['paymentSystemName'] + "\n"

    order_data['observation'] = payment_observation

    sku_data = order_data['sku_data'] = []
    for sku in order_info['items']:
        sku_element = {}
        sku_element['id'] = sku['id']
        sku_element['ref_id'] = sku['refId']
        sku_element['quantity'] = sku['quantity']
        sku_element['price'] = parseToFloatVtex(sku['listPrice'])
        sku_element['discount'] = sku['listPrice'] - sku['price']
        sku_element['discount'] = parseToFloatVtex(sku_element['discount'])
        sku_data.append(sku_element)
    order_list.append(order_data)


def destructuring_client(order_info):
    client_data = order_info['clientProfileData']
    profile_id = client_data['userProfileId']
    profile_url = "https://vetro.vtexcommercestable.com.br/api/dataentities/CL/search?_where=userId={}&_fields=_all".format(
        profile_id)

    client_info = {}
    if profile_id:
        profile_data = get_data(profile_url)
        # select the last profile
        profile_data = profile_data[-1]

        if not profile_data['localeDefault']:
            return
        # getting data about profile id
        client_info['profile_id'] = profile_id
        client_info['full_name'] = "{} {}".format(
            profile_data['firstName'], profile_data['lastName'])
        client_info['email'] = profile_data['email']
        client_info['phone'] = profile_data['phone']
        client_info['cod_country'] = profile_data['localeDefault'].split(
            '-')[1] if profile_data['localeDefault'] else None
        client_info['birth'] = profile_data['birthDate']

        # address info
        address = order_info["shippingData"]['address']
        address_client = client_info['address'] = {}
        address_client['address_id'] = address['addressId']
        address_client['state'] = address['state']
        address_client['city'] = address['city']
        address_client['street'] = address['street']
        address_client['number'] = address['number']
        address_client['complement'] = address['complement']
        address_client['postal_code'] = address['postalCode']
        client_list.append(client_info)


headers = {
    'content-type': "application/json",
    'accept': "application/vnd.vtex.ds.v10+json",
    'x-vtex-api-appkey': api_key,
    'x-vtex-api-apptoken': api_token
}


# url used for api
url_stocks_from_date = "https://vetro.vtexcommercestable.com.br/api/oms/pvt/orders?creationDate=[2020-05-12 TO 2021-05-13]&orderBy=creationDate,asc&page="
url_get_stock = "https://vetro.vtexcommercestable.com.br/api/oms/pvt/orders/"

# iteration ids for orders
flag = True
current_page = 1

# list for clients not repeated
client_list = []
order_list = []

while flag:
    # this request is used to get all the orders on a while
    stock_date = get_data(url_stocks_from_date, current_page)

    # we get each order id
    for order in stock_date['list']:
        order_id = order["orderId"]
        # cocant url with the id founded

        order_info = get_data(url_get_stock, order_id)
        destructuring_client(order_info)
        destructuring_order(order_info, order)
        # get_stock(url_get_stock)

    total_pages = stock_date['paging']['pages']
    current_page = current_page+1
    if current_page > total_pages:
        flag = False

profile_lasted_list = cleaned_last_profiles(client_list)
# print(client_list)
# print(order_list)

# profile_lasted_list = [{'profile_id': '63a23820-3d38-4825-9dcd-d9fcb1091f80', 'full_name': 'TEST DEMO', 'email': 'ancyent@xserv.ro', 'phone': None, 'cod_country': 'RO', 'birth': None, 'address': {'address_id': '4756718155803', 'state': 'IASI', 'city': 'Iasi', 'street': 'Aleea Voda Grigore Ghica', 'number': '25', 'complement': None, 'postal_code': '700000'}},
#                        {'profile_id': 'db9d420e-b757-4cb5-9390-eecee560ff12', 'full_name': 'Laura Dorofte', 'email': 'laura.dorofte@gmail.com', 'phone': None, 'cod_country': 'RO', 'birth': None, 'address': {'address_id': '1585644711906', 'state': 'BOTOSANI', 'city': 'Botosani', 'street': 'muncel', 'number': '7', 'complement': None, 'postal_code': '710000'}}]


# print('(+) Database connection started')
# # database connection
# connection = pyodbc.connect(
#     "Driver={"+db_driver+"};"
#     "Database="+db_name + ";"
#     "Server= "+db_host+","+db_port+";"
#     "UID="+db_user+";"
#     "PWD="+db_password+";"
# )
# connection.setencoding('utf-16-le')

# print('(+) Database connection ended')

# print('(+) Query to database started')
# cursor = connection.cursor()
# cursor.execute(
#     "SELECT [id_importex]  FROM accesex_parteneri_view where [id_importex] like 'vtex-%'")

# # this filled existed vtex ids
# existed_vtex_users = []
# for row in cursor:
#     existed_vtex_users.append(row[0])

# for vtex_user_id in existed_vtex_users:
#     for profile in profile_lasted_list:
#         if vtex_user_id == 'vtex-'+profile['profile_id'][:20]:


# sql_insert = """INSERT INTO [V-TEX.VETRO].dbo.importex_parteneri
# (id_importex, id_intern,
# id_partener, cif_cnp,
# denumire, pers_fizica,
# platitor_tva, registru_comert,
# cass_arenda, banca,
# contul, adresa,
# email, website,
#  fax, telefon,
#  telefon_serv, manager,
# cod_judet, cod_tara,
# id_localitate, den_localitate,
# den_regiune, id_clasificare,
# den_clasificare, id_clasificare2,
# den_clasificare2, id_clasificare3,
# den_clasificare3, id_agent,
# id_extern_agent, den_agent,
# termen_incasare, termen_plata,
# moneda, observatii,
# limita_credit, restanta_max,
# cod_card, id_disc,
# den_disc, id_zona_comerciala,
# den_zona_comerciala,
# client_ret, password,
# errorlist, validare, cod_siruta)
# VALUES(?, ?,
#  ?, ?,
#  ?, ?,
# ?, ?,
# ?, ?,
# ?, ?,
# ?, ?,
# ?, ?,
# ?, ?,
# ?, ?,
# ?, ?,
# ?, ?,
# ?, ?,
# ?, ?,
# ?, ?,
# ?, ?,
# ?, ?,
# ?, ?,
# ?, ?,
# ?, ?,
# ?, ?,
# ?,
# ?, ?,
# ?, ?, ?);
# """


# insert_data = ('vtex-algo', '',
#                'vtex-algo', '',
#                'juan jose', 1,
#                1, '',
#                0, '',
#                '', 'pollo campero sv',
#                'eaxs@as.com', 'website.com',
#                '', '7122-1223',
#                '', '',
#                '', 'RO',
#                0, '',
#                '', '80',
#                '', '',
#                '', '',
#                '', '',
#                '', '',
#                2, 2,
#                'RON', '',
#                0, 0,
#                '', '',
#                '', '1',
#                '',
#                0, '',
#                '', 0, 0)
# cursor.execute(sql_insert, insert_data)
# connection.commit()

# confirm_procedure = "EXEC importex_parteneri_exec @id_importex = N'{}'".format(
#     'vtex-algo')
# cursor.execute(confirm_procedure)
# connection.commit()
# print('(+) data inserted properly')
