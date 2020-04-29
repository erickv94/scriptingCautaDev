from settings import  db_host, db_password, db_port,db_user, db_driver
import pyodbc


connection = pyodbc.connect(
    "Driver={"+db_driver+"};"
    "Server= "+db_host+","+db_port+";"
    "UID="+db_user+";"
    "PWD="+db_password+";"
)

cursor = connection.cursor()
cursor.execute("""SELECT [id_produs], SUM([stoc]) - SUM([rezervat]) AS diff_stoc_reserved 
FROM [S.C. VETRO SOLUTIONS S.R.L.].[dbo].[accesex_stoc_view] WHERE [id_gestiune] = '1(1)' AND [stoc] > 0 
GROUP BY [id_produs];""")

for row in cursor:
    print(row)