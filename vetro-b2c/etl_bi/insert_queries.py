insert_agents = """INSERT INTO public.agents
(agent_id, name)
VALUES(%s, %s);
"""

insert_clients = """INSERT INTO public.clients
(client_id, "name")
VALUES(%s, %s);
"""
insert_brands = """INSERT INTO public.brands
(brand_id, "name")
VALUES(%s, %s);
"""

insert_counties = """INSERT INTO public.counties
(county_id, "name")
VALUES(%s, %s);
"""
insert_locality = """
INSERT INTO public.localities
(locality_id, name)
VALUES(%s, %s);
"""

insert_zone = """
INSERT INTO public.zones
(zone_id, "name")
VALUES(%s, %s);

"""
insert_product = """
INSERT INTO public.products
(product_id, "name", code, "class", unit, tva, brand_id)
VALUES(%s, %s, %s, %s, %s, %s, %s);

"""

insert_invoice = """
INSERT INTO public.invoices
("number", "type", serie, invoice_date, order_id)
VALUES(%s, %s, %s, %s, %s);
"""

insert_invoice_line = """
INSERT INTO public.invoice_lines
(product_id, agent_id, client_id, county_id, locality_id, zone_id, invoice_number, quantity, adquisition_price_unit, sell_price_unit, sell_price_unit_tva, total_discount, adquisition_price_total, sell_price_total,invoice_date)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s);


"""


insert_query_dict = {
    'agent': insert_agents,
    'client': insert_clients,
    'brand': insert_brands,
    'county': insert_counties,
    'locality': insert_locality,
    'zone': insert_zone,
    'product': insert_product,
    'invoice': insert_invoice,
    'invoice_line': insert_invoice_line
}
