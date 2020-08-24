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
# entitie
company = 'MC'
client = 'CL'

# get dataframe for the output from the old store
current_path = Path(__file__).parent.absolute()
skus_xls = current_path/'data.xlsx'
default_sheet = 'Teamshare Export'
df_skus = pd.read_excel(skus_xls, sheet_name=default_sheet)
option = input('What operation need to do: s/r (s=store,r=revoke): ')
# iterate over the dataset
print('(+) iterate over xlsx')
for index, row in df_skus.iterrows():

    # get email from db or xlsx

    if pd.notnull(row['Alternate Username']):

        refId = row["Alternate Username"]
        cursor.execute(query_nexus.format(refId))
        customer = cursor.fetchone()
        # get email from db or xlsx
        emailFromExcel = row['Username']
        email = emailFromExcel if not pd.notnull(
            emailFromExcel) else customer.company_email

        if customer and option.lower() == 's':
            payloadCL = {
                'email': email,
                'companyErpId': customer.company_id,
                'companyName': customer.company_name,
                'companyCIF': customer.cif,
                'firstName': customer.company_name,
                'salesAgentName': customer.agent_name,
                'salesAgentPhone': customer.agent_tel,
                'salesAgentEmail': customer.agent_email
            }

            payloadMC = {
                'email': emailFromExcel if not pd.notnull(emailFromExcel) else customer.company_email,
                'companyErpId': customer.company_id,
                'companyName': customer.company_name,
                'companyNrRegCom': customer.company_reg,
                'companyCIF': customer.cif,
                'salesAgentName': customer.agent_name,
                'salesAgentPhone': customer.agent_tel,
                'salesAgentEmail': customer.agent_email,
                'bank': customer.bank,
                'iban': customer.iban,
                'judet': customer.judet,
                'oras': customer.oras,
                'strada': customer.strada
            }
            payloadCL = json.dumps(payloadCL)
            payloadMC = json.dumps(payloadMC)

            responseCL = requests.put(url.format(
                client), headers=headers, data=payloadCL)
            responseMC = requests.put(url.format(
                company), headers=headers, data=payloadMC)

            print(
                '(+) Row Nr.Crt. id {} processed on masterdata'.format(row['Nr.Crt.']))
            print('\t- http codes CL: {} MC: {}'.format(
                responseCL.status_code, responseMC.status_code))

        elif customer and option.lower() == 'r':

            search_resource_by_email = 'https://vetrob2c.vtexcommercestable.com.br/api/dataentities/{}/search?_fields=id&email={}'
            delete_resource_by_id = 'https://vetrob2c.vtexcommercestable.com.br/api/dataentities/{}/documents/{}'

            # iterate over two entitys, both have an entry point as  an email
            for entity in [client, company]:
                res = requests.get(search_resource_by_email.format(entity, email), headers=headers)
                entityIds = res.json()
                # remove ids from response

                if not len(entityIds) == 0:
                    for entityId in entityIds:
                        id = entityId['id']
                        res = requests.delete(delete_resource_by_id.format(entity, id), headers=headers)
                        print('\t{} document with {} has been removed'.format(entity, id))
                else:
                    print('\t{} document with {} already removed'.format(entity, email))
