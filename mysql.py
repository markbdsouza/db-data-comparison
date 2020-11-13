import pymysql
import json

with open('config.json') as f:
    global_vars = json.load(f)

# Open database connection
db = pymysql.connect(global_vars['MySqlHostName'], global_vars['MySqlUserName'],global_vars['MySqlPassword'],global_vars['MySqlSchema'] )

# prepare a cursor object using cursor() method
cursor = db.cursor()

sql = "SELECT * FROM sakila.actor"

try:
   cursor.execute(sql)
   # Fetch all the rows in a list of lists.
   results = cursor.fetchall()
   for row in results:
      fname = row[0]
      lname = row[1]
      age = row[2]
      # Now print fetched result
      print (fname, lname, age )
except:
   print ("Error: unable to fetch data")

# disconnect from server
db.close()