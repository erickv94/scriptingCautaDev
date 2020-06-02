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
    agent_id = row[11]
    agent_name = row[12]

    brand_id = row[13]
    brand_name = row[14]

    client_id = row[9]
    client_name = row[10]

    county_id = row[18]
    county_name = row[19]

    locality_id = row[16]
    locality_name = row[17]

    zone_id = row[20]
    zone_name = row[21]

    invoice_number = row[]
    invoice_type = row[]
    invoice_serie = row[]
    invoice_date = row[]
    invoice_order_id = row[]

    data_client = (client_id, client_name)
    data_agent = (agent_id, agent_name)

    cursor_psql.execute(insert_agents, data_agent)
    connection_pgsql.commit()
