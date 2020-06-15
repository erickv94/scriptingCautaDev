from datetime import datetime
from datetime import timedelta
now = datetime.now()
yesterday = now - timedelta(days=1)

now_str = now.strftime('%m-%d-%Y')
yesterday_str = yesterday.strftime('%m-%d-%Y')

sql_source = """SELECT 
tip_doc,
seria_doc,
nr_doc,
data_doc,
CONCAT(id_gest, '(',pl_gest,')') AS id_gestiune,
cod_pr,
CONCAT(id_pr, '(',pl_pr, ')') AS id_produs,
denpr AS denumire_produs,
um,
CONCAT(id_cli, '(',pl_cli,')') AS id_client,
nume_cli AS nume_client,
CONCAT(id_pers_part, '(', pl_pers_part,')') AS id_agent,
nume_pers_part AS nume_agent,
CONCAT(id_brand, '(', pl_brand,')') AS id_brand,
nume_brand,
CONCAT(id_cmd, '(', pl_cmd,')') AS id_comanda,
id_loc AS id_localitate,
nume_loc AS nume_localitate,
id_jud AS id_judet,
nume_judet,
CONCAT(id_zona,'(',pl_zona, ')') AS zona,
nume_zona,
culoare AS incadrare_produse,
cantitate,
pu AS pret_intrare,
puv AS pret_vanzare_bucata,
puv_tva AS pret_vanzare_bucata_cu_tva,
val_disc AS valoarea_totala_a_discountului,
pr_tva_o AS cota_tva_procent,

val_pu AS valoarea_totala_a_achizitiilor, 
val_puv AS valoare_totala_a_vanzarii_fara_tva,
den_sup AS denumire_suplimentare


FROM dbo.raport_bi_statistica_vanzari('{}', '{}', 1, 1, 1, 1, 0, 1, 0, 0, 0)
WHERE
cont_cu_denumire NOT LIKE '758%' AND
cont_cu_denumire NOT LIKE '419%' AND
cont_cu_denumire NOT LIKE '557%' AND
cont_cu_denumire NOT LIKE '667%';
""".format(yesterday_str, now_str)

# queries to validate existed data
agent = "select * from public.agents where agent_id = '{}'"
client = "select * from public.clients where client_id = '{}'"

brand = "select * from public.brands where brand_id = '{}'"

county = "select * from public.counties where county_id = '{}'"

locality = "select * from public.localities where locality_id='{}'"

zone = "select * from public.zones where zone_id='{}'"

product = "select * from public.products where product_id='{}' "

invoice = "select * from public.invoices where number='{}'"

sql_select = {
    'agent': agent,
    'client': client,
    'brand': brand,
    'county': county,
    'locality': locality,
    'zone': zone,
    'product': product,
    'invoice': invoice
}
