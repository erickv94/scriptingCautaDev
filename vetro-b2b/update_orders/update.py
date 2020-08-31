from settings import db_name, db_host, db_password, db_port, db_user, db_driver, api_key, api_token
from db import connect_to_db, get_cursor
from insert_data import insert_address, insert_order, get_tva, insert_order
from remove_data import removing_clients, removing_addresses, removing_orders
from pprint import pprint, pformat
import pyodbc
import requests
import time
import json
import hashlib
from datetime import datetime
from datetime import timedelta


def get_data(url, id='master'):

    if id == 'master':
        url_fetch = url
    else:
        url_fetch = url+str(id)

    response = requests.request(
        "GET", url_fetch, headers=headers)

    data = response.json()
    return data

def parseToFloatVtex(value):
    order_total_str = str(value)
    if '.' in order_total_str:
        order_total_str = order_total_str.split('.')[0]

    order_total_float = order_total_str[:-2] + '.' + order_total_str[-2:]
    return float(order_total_float)

def destructuring_order(order_info, order):
    cif = order_info['clientProfileData']['corporateName']
    if not cif:
        return

    order_data = {}
    order_data['profile_id'] = cif
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


def destructuring_address(order_info):
    # address info
    address = order_info["shippingData"]['address']
    address_client = {}

    cif = order_info['clientProfileData']['corporateName']
    address_client['cif'] = cif
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
    address_client['cod_country'] = address['country']
    address_client['phone'] = order_info['clientProfileData']['phone']
    address_list.append(address_client)


headers = {
    'content-type': "application/json",
    'accept': "application/vnd.vtex.ds.v10+json",
    'x-vtex-api-appkey': api_key,
    'x-vtex-api-apptoken': api_token
}

# url used for api
# f_authorizedDate=authorizedDate:[2020-05-15T02:00:00.000Z TO 2020-05-20T01:59:59.999Z]&
url_stocks_from_date = "https://vetrob2c.vtexcommercestable.com.br/api/oms/pvt/orders?f_authorizedDate=authorizedDate:[{} TO {}]&orderBy=creationDate,asc&page=".format(
    '2020-08-29T02:00:00.000Z', "2020-08-31T01:59:59.999Z")

url_get_stock = "https://vetrob2c.vtexcommercestable.com.br/api/oms/pvt/orders/"


# iteration ids for orders
flag = True
current_page = 1
# database env
connection = connect_to_db()
cursor = get_cursor()

tvas_dict = get_tva(connection)

order_list = []
address_list = []
while flag:
    # this request is used to get all the orders on a while
    print('(+) get page {} orders'.format(current_page))

    stock_date = get_data(url_stocks_from_date, current_page)
    for order in stock_date['list']:
        order_id = order["orderId"]
        # cocant url with the id founded
        print('(+) get order {}'.format(order_id))
        order_info = get_data(url_get_stock, order_id)

        destructuring_order(order_info, order)
        destructuring_address(order_info)
        total_pages = stock_date['paging']['pages']
        print('(+) end get {} orders'.format(current_page))
        current_page = current_page+1
        if current_page > total_pages:
            flag = False

for address in address_list:
    insert_address(connection, address)
for order in order_list:
    insert_order(connection, order)
    # insert_order(connection, order)
print(order_list)
