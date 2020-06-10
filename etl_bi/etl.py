from settings import db_host, db_name, db_password, db_port, db_user, db_driver, pg_name, pg_host, pg_port, pg_password, pg_user

import pyodbc
import psycopg2
from read_queries import sql_source
from insert_queries import insert_agents, insert_clients

connection_mssql = pyodbc.connect(
    "Driver={"+db_driver+"};"
    "Server= "+db_host+","+db_port+";"
    "Database="+db_name + ";"
    "UID="+db_user+";"
    "PWD="+db_password+";"
)

connection_pgsql = psycopg2.connect(user=pg_user,
                                    password=pg_password,
                                    host=pg_host,
                                    port=pg_port,
                                    database=pg_name)

cursor_mssql = connection_mssql.cursor()
cursor_psql = connection_pgsql.cursor()


cursor_mssql.execute(sql_source)
data = cursor_mssql.fetchall()


for row in data:
    # agent info
    agent_id = row[11]
    agent_name = row[12]
    # brand info

    brand_id = row[13] if not row[13] == '()' else None
    brand_name = row[14]

    if brand_id:
        print('need to insert brand')

    # product info
    product_id = row[6]
    product_name = row[7]
    product_code = row[5]
    product_class = row[22]
    product_unit = row[8]
    product_tva = row[27]
    product_brand_id = brand_id

    # client info
    client_id = row[9]
    client_name = row[10]
    # county info
    county_id = row[18]
    county_name = row[19]
    # locality info
    locality_id = row[16]
    locality_name = row[17]
    # zone info
    zone_id = row[20]
    zone_name = row[21]
    # invoice
    invoice_number = row[2]
    invoice_type = row[0]
    invoice_serie = row[1]
    invoice_date = row[3]
    invoice_order_id = row[15]

    # invoice line info
    # line_product = row[]
    # line_agent = row[]
    # line_client = row[]
    # line_county = row[]
    # line_locality = row[]
    # line_zone = row[]
    # line_invoice = row[]
    # line_quantity = row[]
    # line_adquisition_price_unit = row[]
    # line_sell_price_unit = row[]
    # line_sell_price_tva = row[]
    # line_total_discount = row[]
    # line_adquisition_price_total = row[]
    # line_sell_price_total = row[]

    data_agent = (agent_id, agent_name)
    data_brand = (brand_id, brand_name)
    data_client = (client_id, client_name)
    data_county = (county_id, county_name)
    data_locality = (locality_id, locality_name)
    data_zone = (zone_id, zone_name)
    data_invoice = (invoice_number, invoice_type,
                    invoice_serie, invoice_date, invoice_order_id)

    # cursor_psql.execute(insert_agents, data_agent)
    # connection_pgsql.commit()
