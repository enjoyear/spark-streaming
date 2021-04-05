import mysql.connector

mydb = mysql.connector.connect(user='root', password='',
                               host='127.0.0.1',
                               database='my_test')
mycursor = mydb.cursor()
mycursor.execute("select * from authors")
for x in mycursor.fetchall():
    print(x)
