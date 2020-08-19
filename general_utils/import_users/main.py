from settings import api_key, api_token, account_vtex, db_name, db_user, db_password, db_host, db_port, db_driver
import requests
import pandas as pd
from pandas import ExcelFile
from pathlib import Path
import pyodbc
import json


headers = {
    'content-type': "application/json",
    'accept': "application/vnd.vtex.ds.v10+json",
    'x-vtex-api-appkey': api_key,
    'x-vtex-api-apptoken': api_token
}

# url to insert entitys
url = "https://vetrob2c.vtexcommercestable.com.br/api/dataentities/{}/documents"


print('(+) Database connection started')
# database connection
connection = pyodbc.connect(
    "Driver={"+db_driver+"};"
    "Server= "+db_host+","+db_port+";"
    "Database="+db_name + ";"
    "UID="+db_user+";"
    "PWD="+db_password+";"
)
cursor = connection.cursor()
print('(+) Database connection ended')

query_nexus = '''select apv.id_intern as company_id, pv.email as company_email, pv.nume as company_name,
	   pv.contul as iban, pv.localitate as oras, pv.adresa as strada, pv.reg_comert as company_reg, 
	   pv.banca as bank, CONCAT(pv.atr_fiscal,' ',pv.cod_fiscal) as cif ,
	   pv.judet as judet , apv.den_agent as agent_name, p.email as agent_email, p.telefon as agent_tel 
	   from parteneri_view  as pv 
	   left join 
	   accesex_parteneri_view apv 
	   on 
	   apv.id_intern = CONCAT(pv.id, '(',pv.pct_lcr,')')
	   left join 
	   personal as p
	   on concat(p.id,'(',p.pct_lcr, ')') = apv.id_agent 
	   where CONCAT(pv.id, '(',pv.pct_lcr,')')= '{}'
       '''
query_personal = 'select * from personal;'
# entitie
company = 'MC'
client = 'CL'

# get dataframe for the output from the old store
current_path = Path(__file__).parent.absolute()
skus_xls = current_path/'data.xlsx'
default_sheet = 'Teamshare Export'
df_skus = pd.read_excel(skus_xls, sheet_name=default_sheet)

# iterate over the dataset
for index, row in df_skus.iterrows():
    if pd.notnull(row['Alternate Username']):
        refId = row["Alternate Username"]
        emailFromExcel = row['Username']

        cursor.execute(query_nexus.format(refId))
        customer = cursor.fetchone()

        # payload={
        #     'email': emailFromExcel if not  pd.notnull(emailFromExcel) else
        # }
        # print(query_nexus.format(refId))
        # customer = cursor.execute(' ')
        # response = requests.put(url, headers=headers, data=payload)
