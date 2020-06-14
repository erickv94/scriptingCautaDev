from settings import db_host, db_name, db_password, db_port, db_user, db_driver, pg_name, pg_host, pg_port, pg_password, pg_user
from parser_data import parser_row_parentheses, parser_row_empty
import pyodbc
import psycopg2
from read_queries import sql_source
from insert_queries import insert_query_dict
from service import create_source
import time

start = time.time()
print('(+) starting connection to SQL Server')
connection_mssql = pyodbc.connect(
    "Driver={"+db_driver+"};"
    "Server= "+db_host+","+db_port+";"
    "Database="+db_name + ";"
    "UID="+db_user+";"
    "PWD="+db_password+";"
)
print('(+) already connected to SQL Server')

print('(+) starting connection to PostgreSQL')
connection_pgsql = psycopg2.connect(user=pg_user,
                                    password=pg_password,
                                    host=pg_host,
                                    port=pg_port,
                                    database=pg_name)

print('(+) already connected to PostgresSQL')


cursor_mssql = connection_mssql.cursor()

print('(+) executing sql query for retrive all data from x to y')
cursor_mssql.execute(sql_source)
data = cursor_mssql.fetchall()
size_data = len(data)

print('(+) data saved on python memory {}'.format(size_data))

count = 0

for row in data:
    # agent info
    agent_id = parser_row_parentheses(row[11])
    agent_name = row[12]

    # brand info
    brand_id = parser_row_parentheses(row[13])
    brand_name = row[14]

    # product info
    product_id = parser_row_parentheses(row[6])
    product_name = row[7]
    product_code = row[5]
    product_class = parser_row_empty(row[22])
    product_unit = row[8]
    product_tva = row[28]
    product_brand_id = brand_id

    # client info
    client_id = parser_row_parentheses(row[9])
    client_name = row[10]

    # county info
    county_id = row[18]
    county_name = row[19]

    # locality info
    locality_id = row[16]
    locality_name = row[17]
    # zone info
    zone_id = parser_row_parentheses(row[20])
    zone_name = row[21]

    # invoice

    invoice_number = row[2]
    invoice_type = row[0]
    invoice_serie = row[1]
    invoice_date = row[3]
    invoice_order_id = parser_row_parentheses(row[15])

    # invoice line info
    line_product = product_id
    line_agent = agent_id
    line_client = client_id
    line_county = county_id
    line_locality = locality_id
    line_zone = zone_id
    line_invoice = invoice_number
    line_quantity = row[23]
    line_adquisition_price_unit = row[24]
    line_sell_price_unit = row[25]
    line_sell_price_tva = row[26]
    line_total_discount = row[27]
    line_adquisition_price_total = row[29]
    line_sell_price_total = row[30]
    line_date = invoice_date

    data_agent = (agent_id, agent_name)
    data_brand = (brand_id, brand_name)
    data_client = (client_id, client_name)
    data_county = (county_id, county_name)
    data_locality = (locality_id, locality_name)
    data_zone = (zone_id, zone_name)
    data_invoice = (invoice_number, invoice_type,
                    invoice_serie, invoice_date, invoice_order_id)

    data_product = (product_id, product_name, product_code,
                    product_class, product_unit, product_tva, product_brand_id)

    data_line_product = (
        line_product, line_agent, line_client, line_county, line_locality, line_zone, line_invoice, line_quantity, line_adquisition_price_unit, line_sell_price_unit, line_sell_price_tva, line_total_discount, line_adquisition_price_total, line_sell_price_total,
        line_date
    )

    data_entity_dict = {
        'agent': data_agent,
        'client': data_client,
        'brand': data_brand,
        'county': data_county,
        'locality': data_locality,
        'zone': data_zone,
        'product': data_product,
        'invoice': data_invoice,
        'invoice_line': data_line_product
    }

    for entity in insert_query_dict.keys():
        create_source(
            connection_pgsql, data_entity_dict[entity], entity, insert_query_dict[entity])
    count = count+1
    if count % 100 == 0:
        print('(+) {} analized'.format(count))

end = time.time()
print('time elapsed')
print(end - start)
