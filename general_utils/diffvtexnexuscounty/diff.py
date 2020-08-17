import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
from pathlib import Path
import datetime
# path defined
current_path = Path(__file__).parent.absolute()

nexus_xls = current_path/'judete_orase_nexus.xlsx'
vtex_xls = current_path/'judete_orase_vtex.xlsx'
path_output_cities = current_path/'result_cities.xlsx'
path_output_counties = current_path/'result_counties.xlsx'

default_sheet = 'Sheet1'

df_nexus = pd.read_excel(nexus_xls, sheet_name=default_sheet)
df_vtex = pd.read_excel(
    vtex_xls, sheet_name=default_sheet)

# needs to have a specific traitment, cause null data
vetex_counties = []
for clean_county in df_nexus['county']:
    if not isinstance(clean_county, float):
        vetex_counties.append(clean_county)

# spreading into lower cases for both of them
nexus_counties = list(set([x.lower().strip() for x in vetex_counties]))
vetex_counties = list(set([x.lower() for x in df_vtex['County'].tolist()]))

nexus_cities = list(set([x.lower().strip()
                         for x in df_nexus['city'].tolist()]))
vetex_cities = list(set([x.lower() for x in df_vtex['City'].tolist()]))

different_counties = []
different_cities = []

for county_v in vetex_counties:
    if not county_v in nexus_counties:
        different_counties.append(county_v)

for city_v in vetex_cities:
    if not city_v in nexus_cities:
        different_cities.append(city_v)


# print(different_cities)
# print(different_counties)

rows_to_printout = []
for index, row in df_vtex.iterrows():
    if row['City'].lower() in different_cities:
        rows_to_printout.append(row)

df_diferent_city = pd.DataFrame(rows_to_printout)
df_diferent_county = pd.DataFrame(different_counties)

writer_cities = pd.ExcelWriter(path_output_cities, engine='xlsxwriter')
writer_counties = pd.ExcelWriter(path_output_counties, engine='xlsxwriter')
df_diferent_city.to_excel(writer_cities, sheet_name='Sheet1', index=False)
df_diferent_county.to_excel(writer_counties, sheet_name='Sheet1', index=False)

writer_counties.save()
writer_cities.save()
