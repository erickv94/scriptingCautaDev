import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
from pathlib import Path
# path defined
current_path=Path(__file__).parent.absolute()

all_products=current_path/'all_products_vtex.xlsx'
product_prices=current_path/'products_prices.xlsx'
path_output=current_path/'products_without_prices.xlsx'

sheet_all_products='Sheet1'
sheet_product_prices='Base prices'


df_all_product = pd.read_excel(all_products, sheet_name=sheet_all_products)
df_products_prices= pd.read_excel(product_prices,sheet_name=sheet_product_prices, skiprows=1)

skus_all=df_all_product['_SkuId (Not changeable)'].tolist()
skus_with_prices=df_products_prices['SKU ID'].tolist()
sku_without_prices=[]


for index, row in df_all_product.iterrows():
     if row['_SkuId (Not changeable)'] not in skus_with_prices:
         sku_without_prices.append(row)


df_without_prices=pd.DataFrame(sku_without_prices)

writer = pd.ExcelWriter(path_output, engine='xlsxwriter')
df_without_prices.to_excel(writer, sheet_name='Sheet1', index=False)


writer.save()