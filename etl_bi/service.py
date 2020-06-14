from read_queries import sql_select


def create_source(ctx, data, entity, sql_query):
    cursor = ctx.cursor()
    primary_key = data[0]

    if entity == 'invoice_line':
        cursor.execute(sql_query, data)
        return None

    if primary_key or primary_key == 0:
        if not exist_data(ctx, entity, primary_key):
            cursor.execute(sql_query, data)
            ctx.commit()
    else:
        print('primary key is null on entity {}'.format(entity))


def exist_data(ctx, entity, id):
    cursor = ctx.cursor()
    cursor.execute(sql_select[entity].format(id))
    el = cursor.fetchone()

    return True if el else False
