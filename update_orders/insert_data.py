def insert_client(ctx, client):
    sql_insert = """INSERT INTO [V-TEX.VETRO].dbo.importex_parteneri
    (id_importex, id_intern,
    id_partener, cif_cnp,
    denumire, pers_fizica,
    platitor_tva, registru_comert,
    cass_arenda, banca,
    contul, adresa,
    email, website,
    fax, telefon,
    telefon_serv, manager,
    cod_judet, cod_tara,
    id_localitate, den_localitate,
    den_regiune, id_clasificare,
    den_clasificare, id_clasificare2,
    den_clasificare2, id_clasificare3,
    den_clasificare3, id_agent,
    id_extern_agent, den_agent,
    termen_incasare, termen_plata,
    moneda, observatii,
    limita_credit, restanta_max,
    cod_card, id_disc,
    den_disc, id_zona_comerciala,
    den_zona_comerciala,
    client_ret, password,
    errorlist, validare, cod_siruta)
    VALUES(?, ?,
    ?, ?,
    ?, ?,
    ?, ?,
    ?, ?,
    ?, ?,
    ?, ?,
    ?, ?,
    ?, ?,
    ?, ?,
    ?, ?,
    ?, ?,
    ?, ?,
    ?, ?,
    ?, ?,
    ?, ?,
    ?, ?,
    ?, ?,
    ?, ?,
    ?, ?,
    ?, ?,
    ?,
    ?, ?,
    ?, ?, ?);
    """
    # creating vtex_id
    vtex_id = 'vtex-'+client['profile_id']
    # get address
    address_info = client['address']
    address = address_info['state']+' '+address_info['city'] + \
        ' '+address_info['street']+address_info['number']

    # get phpne
    phone = client['phone'] if client['phone'] else ''
    # print(vtex_id)
    insert_data = (vtex_id, '',
                   vtex_id, '',
                   client['full_name'], 1,
                   1, '',
                   0, '',
                   '', address,
                   client['email'], '',
                   '', phone,
                   '', '',
                   '', client['cod_country'],
                   0, client['address']['city'],
                   client['address']['state'], '82',
                   '', '',
                   '', '',
                   '', '',
                   '', '',
                   2, 2,
                   'RON', '',
                   0, 0,
                   '', '',
                   '', '1',
                   '',
                   0, '',
                   '', 1, 0)

    cursor = ctx.cursor()
    cursor.execute(sql_insert, insert_data)
    ctx.commit()
    queryExist = "SELECT * FROM accesex_parteneri_view WHERE id_importex= '{}'".format(
        vtex_id)
    cursor.execute(queryExist)
    client_cursor = cursor.fetchone()
    # if the vtex-id exist on parteneri this will be updated at once
    if client_cursor:
        confirm_procedure = "EXEC importex_parteneri_exec @id_importex = N'{}', @manage_existing = N'1'".format(
            vtex_id)
    else:
        confirm_procedure = "EXEC importex_parteneri_exec @id_importex = N'{}'".format(
            vtex_id)

    cursor.execute(confirm_procedure)
    ctx.commit()
    print('(+) data inserted/updated  {} properly'.format(vtex_id))


def insert_address(ctx, address):
    sql_insert = """INSERT INTO [V-TEX.VETRO].[dbo].importex_adrese
	(id_importex, id_extern,
    denumire, id_extern_partener,
    cod_tara, den_localitate,
    den_regiune, strada,
    numar, telefon,
    observatii)
    VALUES
	(?, ?,
     ?, ?,
	 ?, ?,
     ?, ?,
     ?, ?,
     ?)
    """

    client_id = next(iter(address.keys()))
    address_list = address[client_id]

    for address in address_list:

        vtex_client_id = 'vtex-'+client_id
        vtex_address_id = 'vtex-'+address['address_id']
        complement = address['complement'] if address['complement'] else None
        phone = address['phone'] if address['phone'] else ''
        denumire = address['full_name']
        if complement:
            observati = address['street'] + ' nr. ' + \
                address['number']+', '+complement
        else:
            observati = address['street'] + ' nr. ' + \
                address['number']

        insert_data = (vtex_address_id, vtex_address_id,
                       denumire, vtex_client_id,
                       address['cod_country'], address['city'],
                       address['state'], address['street'],
                       address['number'], phone,
                       observati)
        print(insert_data)
        cursor = ctx.cursor()
        cursor.execute(sql_insert, insert_data)
        ctx.commit()
        queryExist = "SELECT * FROM [V-TEX.VETRO].dbo.accesex_adrese_parteneri_view WHERE id_importex= '{}'".format(
            vtex_address_id)
        cursor.execute(queryExist)
        client_cursor = cursor.fetchone()
        # if the vtex-id exist on parteneri this will be updated at once
        if client_cursor:
            confirm_procedure = "EXEC importex_adrese_exec @id_importex = N'{}', @manage_existing = N'1'".format(
                vtex_address_id)
        else:
            confirm_procedure = "EXEC importex_adrese_exec @id_importex = N'{}'".format(
                vtex_address_id)

        cursor.execute(confirm_procedure)
        ctx.commit()
        print('(+) data inserted/updated  {} properly (address)'.format(vtex_address_id))


def insert_orders(ctx, order):
    sql_insert_order = """
    INSERT INTO [V-TEX.VETRO].dbo.importex_comenzi_clienti
    (id_importex, id_document, 
     tip_document,id_carnet,
    serie_document, data_document, 
    data_valabil, data_livrare, 
    scadenta, moneda,
    id_gestiune, id_extern_client,
    id_agent, id_extern_adresa, 
    den_adresa, rezervare, 
    aprobare, taxare_inversa,
    validare, observatii, 
    discount, discount_proc,
    valoare, blocare_aplicare_promo)
    VALUES(?, ?,
           ?, ?,
           ?, ?,
           ?, ?,
           ?, ?, 
           ?, ?,
           ?, ?,
           ?, ?,
           ?, ?, 
           ?, ?,
           ?, ?,  
           ?, ?)
    """
    sql_insert_order_line = ''


def get_tva(ctx):
    query = "SELECT id_intern, cota_tva_ies FROM accesex_produse_view WHERE tip = 'N';"
    cursor = ctx.cursor()
    cursor.execute(query)
    tvas = cursor.fetchall()
    return dict(tvas)
