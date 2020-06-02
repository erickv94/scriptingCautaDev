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
