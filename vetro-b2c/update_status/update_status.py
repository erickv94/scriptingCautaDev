from settings import db_name, db_host, db_password, db_port, db_user, db_driver, api_key, api_token
import pyodbc
import requests
import time
import json
from datetime import date


def validate_status(status_list, status):
    for status_iter in status_list:
        stat = status_iter.strip()
        if stat == status.strip():
            return True

    return False


print('(+) Database connection started')

# api headers
headers = {
    'content-type': "application/json",
    'accept': "application/vnd.vtex.ds.v10+json",
    'x-vtex-api-appkey': api_key,
    'x-vtex-api-apptoken': api_token
}

# database connection
connection = pyodbc.connect(
    "Driver={"+db_driver+"};"
    "Database="+db_name + ";"
    "Server= "+db_host+","+db_port+";"
    "UID="+db_user+";"
    "PWD="+db_password+";"
)
connection.setencoding('utf-16-le')

cursor = connection.cursor()
# SQL PART FROM NEXUS

# getting all new vtex- order from nexuss
query_vtex_nexus = "select id_importex,stare_comanda, id_document from accesex_comenzi_clienti where id_importex  like 'vtex-%' and data_document='{}' ".format(
    str(date.today()))


# query to get series and number
query_invoice_nexus = """
SELECT acc.id_importex, acc.stare_comanda, accl.id_produs, afc.numar_document, afc.serie_document , afc.data_document, afc.numar_awb, afc.id_arhiva_factura
FROM accesex_comenzi_clienti acc
INNER JOIN accesex_comenzi_clienti_lin accl ON accl.id_document = acc.id_document
LEFT JOIN accesex_facturi_clienti_lin afcl ON afcl.id_comanda = CONCAT(acc.id,'(',acc.pct_lcr,')') AND afcl.id_produs = accl.id_produs
LEFT JOIN accesex_facturi_clienti afc ON afc.id_document = afcl.id_document

WHERE acc.id_document = '{}'

"""

# retriving order
url_get_order = 'https://vetro.vtexcommercestable.com.br/api/oms/pvt/orders/{}'
# post notification
url_post_invoice_notification = 'https://vetro.vtexcommercestable.com.br/api/oms/pvt/orders/{}/invoice'
# cancel order
url_post_cancel_order = "https://vetro.vtexcommercestable.com.br/api/oms/pvt/orders/{}/cancel"
# ready for handling order

url_post_ready = "https://vetro.vtexcommercestable.com.br/api/oms/pvt/orders/{}/start-handling"

cursor.execute(query_vtex_nexus)
orders_status = cursor.fetchall()

status_invoice = ['Onorata, Incasata, Inchisa',
                  'Onorata partial, Inchisa',
                  'Onorata partial, Incasata, Inchisa',
                  'Onorata, Inchisa',
                  'Onorata partial',
                  'Onorata']

status_ready_for_handling = ['Neprocesata', 'Ferma']

status_cancelled_order = [
    'Ferma, Incasata, Inchisa', 'Anulata', 'Ferma, Inchisa']


for order in orders_status:
    vtex_id = order[0].split('vtex-')[1]
    status = order[1]
    number_order = order[2]

    print("(+) Requesting order {}".format(vtex_id))

    response = requests.request(
        "GET", url_get_order.format(vtex_id), headers=headers)
    order_details = response.json()

    vtex_status = order_details['status']
    vtex_date_invoice = order_details['invoicedDate']

    if validate_status(status_invoice, status) and not vtex_status == 'invoiced' and not vtex_date_invoice:
        print("(+) Updating state on vtex order {} [invoice]".format(vtex_id))

        value = order_details['value']

        print(' - executing query this takes along time')
        cursor.execute(query_invoice_nexus.format(number_order))
        invoice = cursor.fetchone()
        date_invoice = invoice[5]
        serie = invoice[4]
        number = invoice[3]

        if number and serie:
            # building payload
            payload = {}
            payload['invoiceNumber'] = serie+'-'+str(number)
            payload['invoiceValue'] = value
            payload['issuanceDate'] = str(date_invoice)
            payload['invoiceUrl'] = "https://dev.vetro.vet/externe/json_api/read_factura.php?serie_factura={}&numar_document={}".format(
                serie, str(number_order))
            payload = json.dumps(payload)
            print(' - sending invoice notification')
        response = requests.request('POST', url_post_invoice_notification.format(
            vtex_id), data=payload, headers=headers)
        response = requests.request('POST', url_post_ready.format(
            vtex_id), data=payload, headers=headers)

    if validate_status(status_cancelled_order, status) and not vtex_status == 'cancelled':
        print(
            "(+) Updating state on vtex order {} [cancelled]".format(vtex_id))
        response = requests.request(
            'POST', url_post_cancel_order.format(vtex_id), headers=headers)

    if validate_status(status_ready_for_handling, status) and not vtex_status == 'ready-for-handling':
        print(
            "(+) Updating state on vtex order {} [ready-for-handling]".format(vtex_id))
        response = requests.request(
            'POST', url_post_ready.format(vtex_id), headers=headers)
