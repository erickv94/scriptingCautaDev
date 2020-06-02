from settings import db_name, db_host, db_password, db_port, db_user, db_driver, api_key, api_token
from db import connect_to_db, get_cursor
from insert_data import insert_client, insert_address, insert_order, get_tva
from remove_data import removing_clients, removing_addresses, removing_orders
from pprint import pprint, pformat
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
    # not acept rmn and canceled orders
    order_data['address_id'] = 'vtex-' + \
        order_info["shippingData"]['address']['addressId']

    if 'canceled' in order_info['status'] or 'RMN-' in order_data['order_id'] or 'window-to-cancel' in order_info['status']:
        return
    print('(+) destructuring order')

    order_data['creation_date'] = order_info['creationDate']
    order_data['estimation_date'] = order['ShippingEstimatedDateMax']

    splited_creation_date = order_data['creation_date'].split('T')
    time = splited_creation_date[1].split('.')[0]
    order_data['creation_date'] = splited_creation_date[0]

    splited_estimation_date = order_data['estimation_date'].split('T')
    time = splited_estimation_date[1].split('.')[0]
    order_data['estimation_date'] = splited_estimation_date[0]+' '+time
    order_data['total'] = parseToFloatVtex(
        order['totalValue'])  # needs to parse
    payment_data = order_info['paymentData']
    payment_observation = ''

    for order in order_info['totals']:
        if order['id'] == 'Shipping':
            order_data['shipping_price'] = parseToFloatVtex(order['value'])

    for transactions in payment_data['transactions']:
        for payment in transactions['payments']:
            payment_observation = payment_observation + \
                "paymentSystemName: " + payment['paymentSystemName'] + "\n"

    order_data['observation'] = payment_observation

    sku_data = order_data['sku_data'] = []

    for sku in order_info['items']:
        sku_element = {}
        sku_element['id'] = sku['id']
        sku_element['sku_name'] = sku['name']
        sku_element['ref_id'] = sku['refId']
        sku_element['quantity'] = sku['quantity']
        sku_element['price'] = parseToFloatVtex(sku['listPrice'])
        sku_element['discount'] = sku['listPrice'] - sku['price']
        sku_element['discount'] = parseToFloatVtex(sku_element['discount'])

        if sku_element['discount']:
            percent_tva = float(tvas_dict[sku['refId']]/100)
            discount = sku_element['discount']
            discount = round(discount-discount*percent_tva, 2)
            sku_element['discount'] = discount
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
        address['state'] = address['state'].lower().replace('ş', 's')
        address_client['state'] = address['state']
        # replacing conflicts chars
        address['city'] = address['city'].lower().replace('ş', 's')
        address['city'] = address['city'].replace('(', '')
        address['city'] = address['city'].replace(')', '')
#
        address_client['city'] = address['city']
        address_client['street'] = address['street']
        address_client['number'] = address['number']
        address_client['complement'] = address['complement']
        address_client['postal_code'] = address['postalCode']
        address_client['cod_country'] = client_info['cod_country']
        address_client['phone'] = client_info['phone']
        address_client['full_name'] = client_info['full_name']

        client_list.append(client_info)


def collect_address(client_list):
    clients = []
    clients_dict = {}
    # appending addresses to each client
    for client in client_list:
        if client['profile_id'] in clients_dict.keys():
            clients_dict[client['profile_id']].append(
                client["address"])  # x: [1], y: [1,2,3]
        else:
            addresses = clients_dict[client["profile_id"]] = []
            addresses.append(client['address'])

    # creating a list of not repeatead addresses into each client
    for key in clients_dict.keys():
        addresses_client = {}
        # avoiding repeated
        for address in clients_dict[key]:  # [1,1,2,3]
            if address['address_id'] not in addresses_client.keys():
                # {1: cont,2: cont,3: cont} rigth now is {x:[], y:[]} not readable
                addresses_client[address['address_id']] = address
        # convert into an list
        clients_dict[key] = []
        for address in addresses_client.values():
            clients_dict[key].append(address)

        client_dict = {}
        client_dict[key] = clients_dict[key]
        clients.append(client_dict)

    return clients


headers = {
    'content-type': "application/json",
    'accept': "application/vnd.vtex.ds.v10+json",
    'x-vtex-api-appkey': api_key,
    'x-vtex-api-apptoken': api_token
}


# url used for api
# f_authorizedDate=authorizedDate:[2020-05-15T02:00:00.000Z TO 2020-05-20T01:59:59.999Z]&
url_stocks_from_date = "https://vetro.vtexcommercestable.com.br/api/oms/pvt/orders?f_authorizedDate=authorizedDate:[2020-06-02T06:00:00.000Z TO 2020-06-02T09:59:59.999Z]&orderBy=creationDate,asc&page="
url_get_stock = "https://vetro.vtexcommercestable.com.br/api/oms/pvt/orders/"

# iteration ids for orders
flag = True
current_page = 1
# database env
connection = connect_to_db()
cursor = get_cursor()

tvas_dict = get_tva(connection)
# global variables to use not destructured
client_list = []
order_list = []

# only needed if you want to deleted, each entity on db
# removing_orders(connection)
# removing_clients(connection)
# removing_addresses(connection)

while flag:
    # this request is used to get all the orders on a while
    print('(+) get page {} orders'.format(current_page))

    stock_date = get_data(url_stocks_from_date, current_page)

    # we get each order id
    for order in stock_date['list']:
        order_id = order["orderId"]
        # cocant url with the id founded
        print('(+) get order {}'.format(order_id))
        order_info = get_data(url_get_stock, order_id)

        destructuring_client(order_info)
        destructuring_order(order_info, order)
        # get_stock(url_get_stock)

    total_pages = stock_date['paging']['pages']
    print('(+) end get {} orders'.format(current_page))
    current_page = current_page+1
    if current_page > total_pages:
        flag = False
# print(order_list)
# destructuring data for insert into nexus
profile_lasted_list = cleaned_last_profiles(client_list)
address_per_client = collect_address(client_list)

# print(profile_lasted_list)
# this will insert each profile into vtex
for profile in profile_lasted_list:
    insert_client(connection, profile)
for address in address_per_client:
    insert_address(connection, address)
for order in order_list:
    insert_order(connection, order)
