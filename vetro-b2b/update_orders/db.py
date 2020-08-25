from settings import db_name, db_host, db_password, db_port, db_user, db_driver
import pyodbc


def connect_to_db():
    print('(+) Database connection started')

    # database connection
    connection = pyodbc.connect(
        "Driver={"+db_driver+"};"
        "Database="+db_name + ";"
        "Server= "+db_host+","+db_port+";"
        "UID="+db_user+";"
        "PWD="+db_password+";"
    )

    return connection


def get_cursor():
    ctx = connect_to_db()
    cursor = ctx.cursor()
    return cursor
