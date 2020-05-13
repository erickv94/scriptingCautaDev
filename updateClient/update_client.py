from settings import db_name, db_host, db_password, db_port, db_user, db_driver, api_key, api_token
import pyodbc
import requests
import time
import json


def get_data(url, id='master'):

    if id == 'master':
        url_fetch = url
    else:
        url_fetch = url+str(id)

    response = requests.request(
        "GET", url_fetch, headers=headers)

    data = response.json()
    return data


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
url_stocks_from_date = "https://vetro.vtexcommercestable.com.br/api/oms/pvt/orders?creationDate=[2020-01-01 TO 2021-01-01]&orderBy=creationDate,asc&page="
url_get_stock = "https://vetro.vtexcommercestable.com.br/api/oms/pvt/orders/"

# iteration ids for orders
flag = True
current_page = 1

# list for clients not repeated
client_list = []

while flag:
    # this request is used to get all the orders on a while
    stock_date = get_data(url_stocks_from_date, current_page)

    # we get each order id
    for order in stock_date['list']:
        order_id = order["orderId"]
        # cocant url with the id founded

        order_info = get_data(url_get_stock, order_id)
        destructuring_client(order_info)
        # get_stock(url_get_stock)

    total_pages = stock_date['paging']['pages']
    current_page = current_page+1
    if current_page > total_pages:
        flag = False

print(client_list)
