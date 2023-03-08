
import pymssql

conn = pymssql.connect(server='eu-az-sql-serv1.database.windows.net',
                       user='ux0pv970bzf8zzx',
                       password='lIbQnEIiHmV751!@2J7JOK2YO',
                       database='dlavnozlvyw33no')

cursor = conn.cursor()
cursor.execute("SELECT TOP 2 * FROM Addepar.Holdings")
row = cursor.fetchone()
while row:
    print(row)
    row = cursor.fetchone()
