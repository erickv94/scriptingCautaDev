def removing_clients(ctx):
    cursor = ctx.cursor()
    print('(+) Removing data from database and vtex [clients]')
    cursor.execute(
        "SELECT [id_importex]  FROM accesex_parteneri_view where [id_importex] like 'vtex-%'")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
        print(row[0])
        sql = "EXEC importex_parteneri_anulare @id_importex = N'{}'".format(
            row[0])
        cursor.execute(sql)
        ctx.commit()

    print('(+) Removed clients from database')


def removing_addresses(ctx):
    cursor = ctx.cursor()
    print('(+) Removing data from database and vtex [addresses]')
    cursor.execute(
        "SELECT [id_importex]  FROM accesex_adrese_parteneri_view where [id_importex] like 'vtex-%'")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
        print(row[0])
        sql = "EXEC importex_adrese_anulare @id_importex = N'{}'".format(
            row[0])
        cursor.execute(sql)
        ctx.commit()

    print('(+) Removed addresses from database')


def removing_orders(ctx):
    cursor = ctx.cursor()
    print('(+) Removing data from database and vtex [orders]')
    cursor.execute(
        "SELECT [id_importex]  FROM accesex_comenzi_clienti where [id_importex] like 'vtex-%'")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
        print(row[0])
        sql = "EXEC importex_comenzi_clienti_anulare @id_importex = N'{}'".format(
            row[0])
        cursor.execute(sql)
        ctx.commit()
    cursor.execute(
        "SELECT [id_importex]  FROM accesex_comenzi_clienti where [id_importex] like 'vtex-%'")
    print('(+) Removed orders from database')
