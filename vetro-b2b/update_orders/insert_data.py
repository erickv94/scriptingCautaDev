
import random
import string
import smtplib
from settings import carnet_id, smtp_server, smtp_user, smtp_password, smtp_port, first_destinatary


def randomword(length):
    letters = string.ascii_lowercase

    return ''.join(random.choice(letters) for i in range(length))


def insert_address(ctx, address):
    cursor = ctx.cursor()
    sql_insert = """INSERT INTO importex_adrese
	(id_importex, id_extern,
    denumire, id_partener,
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
    sql_fetch_id_company = """select  apv.id_intern  as customer_id, CONCAT(pv.atr_fiscal,' ',pv.cod_fiscal) as cif 
    from parteneri_view  as pv 
    inner join 
    accesex_parteneri_view apv 
    on 
    apv.id_intern = CONCAT(pv.id, '(',pv.pct_lcr,')') where  CONCAT(pv.atr_fiscal,' ',pv.cod_fiscal)='{}'
    """
    print(sql_insert)
    cursor.execute(sql_fetch_id_company.format(address['cif']))
    company = cursor.fetchone()
    if not company:
        return
    vtex_address_id = 'vtex-'+address['address_id']+'-js'
    complement = address['complement'] if address['complement'] else None
    phone = address['phone'] if address['phone'] else ''
    denumire = address['street']+' '+address['number']+' '+randomword(3)
    if complement:
        observati = address['street'] + ' nr. ' + \
            address['number']+', '+complement
    else:
        observati = address['street'] + ' nr. ' + \
            address['number']

    insert_data = (vtex_address_id, vtex_address_id,
                   denumire, company.customer_id,
                   'RO', address['city'],
                   address['state'], address['street'],
                   address['number'], phone,
                   observati)
    print(insert_data)
    cursor.execute(sql_insert, insert_data)
    ctx.commit()
    queryExist = "SELECT * FROM accesex_adrese_parteneri_view WHERE id_importex= '{}'".format(
        vtex_address_id)
    cursor.execute(queryExist)
    exist = cursor.fetchone()
    # if exist:
    confirm_procedure = "EXEC importex_adrese_exec @id_importex = N'{}', @manage_existing = N'1' , @keep_data_on_err = N'0', @updated_columns = N'{}'; ".format(
        vtex_address_id, 'id_importex,id_extern,denumire,id_partener,cod_tara,den_localitate,den_regiune,strada,numar,telefon,observatii')
    print(confirm_procedure)
    # else:
    #     confirm_procedure = "EXEC importex_adrese_exec @id_importex = N'{}', @keep_data_on_err = N'0' ".format(
    #         vtex_address_id)
    try:
        cursor.execute(confirm_procedure)
        ctx.commit()
        print(
            '(+) data inserted/updated  {} properly (address)'.format(vtex_address_id))
    except Exception as e:
        print(e)
        # message = "Subject: {} \n\n An error has ocurred on address with state: {} city: {} error: {} \n\n Query used: {} with data {}".format(
        #     "Error on vtex-nexus integration [address]", address['state'], address['city'], e, sql_insert, insert_data)
        # server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        # server.login(smtp_user, smtp_password)
        # server.sendmail('cauta-vet-noreply@cauta.vet',
        #                 first_destinatary, message)


def insert_order(ctx, order):
    sql_insert_order = """
    INSERT INTO importex_comenzi_clienti
    (id_importex, numar_document,
     tip_document,id_carnet,
    serie_document, data_document,
    data_valabil, data_livrare,
    scadenta, moneda,
    id_gestiune, id_client,
    id_extern_adresa, den_adresa,
    rezervare, aprobare,
    taxare_inversa, validare,
    observatii, discount,
    discount_proc, valoare,
    blocare_aplicare_promo)
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
           ?)
    """
    print(sql_insert_order)
    sql_insert_order_line = """
    INSERT INTO importex_comenzi_clienti_lin
    (id_document, id_produs,
    tip_produs,  cantitate,
    pret_vanzare, pret_vanzare_tva,
    discount, discount_proc,
    den_produs)

    VALUES (
        ?,?,
        ?,?,
        ?,?,
        ?,?,
        ?
    )
    """
    sql_fetch_id_company = """select  apv.id_intern  as customer_id, CONCAT(pv.atr_fiscal,' ',pv.cod_fiscal) as cif 
    from parteneri_view  as pv 
    inner join 
    accesex_parteneri_view apv 
    on 
    apv.id_intern = CONCAT(pv.id, '(',pv.pct_lcr,')') where  CONCAT(pv.atr_fiscal,' ',pv.cod_fiscal)='{}'
    """

    id_document = get_document_id(ctx)
    vtex_order_id = 'vtex-'+order['order_id']+'nx'
    cif = order['profile_id']
    vtex_address_id = order['address_id']

    cursor = ctx.cursor()
    # cursos before of it
    cursor.execute(sql_fetch_id_company.format(cif))
    company = cursor.fetchone()
    if not company:
        return

    order_data = (vtex_order_id, id_document,
                  'Comanda client', carnet_id,
                  'VETB2B', order['creation_date'],
                  order['creation_date'], order['estimation_date'],
                  0, 'RON',
                  '1(1)', company.customer_id,
                  vtex_address_id, '',
                  1, 1,
                  0, 1,
                  order['observation'], 0,
                  0, order['total'],
                  0
                  )
    print(order_data)
    print(sql_insert_order_line)
    cursor.execute(sql_insert_order, order_data)
    ctx.commit()

    sql_exist_order = "select * from accesex_comenzi_clienti  where id_importex = '{}'".format(
        vtex_order_id)

    cursor.execute(sql_exist_order)
    exist = cursor.fetchone()

    if not exist:
        for sku in order['sku_data']:
            order_line_data = (id_document, sku['ref_id'],
                               'FAA', sku['quantity'],
                               '', sku['price'],
                               sku['discount'], '0',
                               sku['sku_name']
                               )
            cursor.execute(sql_insert_order_line, order_line_data)
            ctx.commit()
            print(order_line_data)

        # transport item for invoices
        order_line_data = (id_document, "3612(1)",
                           'FAA', 1,
                           '', order['shipping_price'],
                           0, '0',
                           "Shipping")
        print(order_line_data)
        cursor.execute(sql_insert_order_line, order_line_data)
        ctx.commit()
        # using procedure
        confirm_procedure = "EXEC importex_comenzi_clienti_exec @id_importex = N'{}' , @keep_data_on_err = N'0' ".format(
            vtex_order_id)
        print(confirm_procedure)
        try:
            cursor.execute(confirm_procedure)
            ctx.commit()
            print('(+) data inserted  {} properly (order)'.format(vtex_order_id))
        except Exception as e:
            print(e)
            # using exception for client message
            # message = "Subject: {} \n\n An error has ocurred on address with  order_id: {} user_id: {} error: {} \n\n Query Used: {} with values {}".format(
            #     "Error on vtex-nexus integration [order]", vtex_order_id, company.customer_id, e, sql_insert_order, order_data)
            # server = smtplib.SMTP_SSL(smtp_server, smtp_port)
            # server.login(smtp_user, smtp_password)
            # server.sendmail('cauta-vet-noreply@cauta.vet',
            #                 first_destinatary, message)
    else:
        print('(+) order already inserted  {} properly (order)'.format(vtex_order_id))


def get_tva(ctx):

    query = "SELECT id_intern, cota_tva_ies FROM accesex_produse_view WHERE tip = 'N';"
    cursor = ctx.cursor()
    cursor.execute(query)
    tvas = cursor.fetchall()
    return dict(tvas)


def get_document_id(ctx):

    sequence_document_query = "SELECT IDENT_CURRENT('importex_comenzi_clienti') + 1 "
    cursor = ctx.cursor()
    cursor.execute(sequence_document_query)
    data = cursor.fetchone()
    document_id = data[0]
    return int(document_id)
