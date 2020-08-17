from settings import api_key, api_token, account_vtex
import requests
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
from pathlib import Path
from io import BytesIO
import imagehash
from PIL import Image

# api headers
headers = {
    'content-type': "application/json",
    'accept': "application/vnd.vtex.ds.v10+json",
    'x-vtex-api-appkey': api_key,
    'x-vtex-api-apptoken': api_token
}

# info for sku including urls
url_to_retrieves_sku_image = 'https://vetrob2c.vtexcommercestable.com.br/api/catalog_system/pvt/sku/stockkeepingunitbyid/{}'

url_image_generic = 'https://vetrob2c.vteximg.com.br/arquivos/ids/156259-55-55/image-62bd8bc7fb3c409d89b6642679015d15.jpg?v=637277008159200000'

# get hash for generic img
print('* Get hash image generic')
response = requests.get(url_image_generic)
img_generic = Image.open(BytesIO(response.content))
hash_generic = imagehash.average_hash(img_generic)

current_path = Path(__file__).parent.absolute()
skus_xls = current_path/'skus_list.xls'
path_output_skus_without_images = current_path/'skus_without_image.xlsx'

default_sheet = 'Sheet1'

df_skus = pd.read_excel(skus_xls, sheet_name=default_sheet)

rows_to_printout = []
print('* Start to get data')
for index, sku in df_skus.iterrows():
    sku_id = sku['_SkuId (Not changeable)']
    if isinstance(sku_id, int):
        # getting sku data
        res = requests.get(
            url_to_retrieves_sku_image.format(sku_id), headers=headers)
        # gettting hash for image response
        res = requests.get(res.json()['ImageUrl'])
        img = Image.open(BytesIO(res.content))
        hash_temp = imagehash.average_hash(img)

        # it will match the current hash with the generic one
        if hash_temp == hash_generic:
            rows_to_printout.append(sku)
    print('** Sku_id {} was processed '.format(sku_id))

df_out = pd.DataFrame(rows_to_printout)
writer_output = pd.ExcelWriter(
    path_output_skus_without_images, engine='xlsxwriter')
df_out.to_excel(writer_output, sheet_name='Sheet1', index=False)

writer_output.save()
