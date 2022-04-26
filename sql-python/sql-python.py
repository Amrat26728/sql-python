import mysql.connector as mysql
from mysql.connector.errors import Error
import pandas as pd

# this method creates the connection to the MySQL server.
def create_server_connection(host_name, user_name, user_pass, db_name):
     connection = None
     try:
          connection = mysql.connect(
               host = host_name, 
               user = user_name, 
               password = user_pass,
               # we can also create connection with database directly while building connection with mysql.
               database = db_name
               )
          print("connection successful!")
     except Error:
          print(Error)
     return connection

# this method creates the connection to the database.
def create_database(connection, query):
     cursor = connection.cursor()
     try:
          cursor.execute(query)
          print("Database created successfully!")
     except Error:
          print(Error)

# this method creates the table in the database.
def create_table(connection, query):
     cursor = connection.cursor()
     try:
          # cursor.execute("use students")  # database can also be selected separatly like this.
          cursor.execute(query)
          print("table created successfully!")
     except Error:
          print(Error)

def insert_data(connection, query):
     cursor = connection.cursor()
     try:
          cursor.execute(query)
          # commit can done with two ways:
          # cursor.execute("commit")
          connection.commit()
          print("data inserted successfully!")
     except Error:
          print(Error)

def get_all_records(connection, query):
     cursor = connection.cursor()
     try:
          cursor.execute(query)
          # fetchall() method takes all records and returns all records in list fromat.
          results = cursor.fetchall()  
          return results
     except Error:
          print(Error)

def get_particular_columns(connection, query):
     cursor = connection.cursor()
     try:
          cursor.execute(query)
          result = cursor.fetchall()
          return result
     except Error:
          print(Error)

connection = create_server_connection("localhost", "root", "amrat26728", "students")

# below is the query to create the database.
# create_db_query = "create database students"
# create_database(connection, create_db_query)

# below is the query to create table.
# create_table_query = "create table 19_batch(rollno varchar(10), name varchar(30), department varchar(30))"
# create_table(connection, create_table_query)

# below is the query to insert data.
# insert_dt = "insert into 19_batch values('19sw26', 'Noman', 'Software')"
# insert_dt = "insert into 19_batch values('19sw34', 'Mahmood', 'Mechanical')"
# insert_dt = "insert into 19_batch values('19sw12', 'Asif', 'BBA')"
# insert_dt = "insert into 19_batch values('19sw43', 'Amrat', 'Software')"

# insert_data(connection, insert_dt)

# get all data from database
# get_records_query = "select * from 19_batch"
# print(records)
# for record in records:
#      print(record)

# getting particular columns from database
# get_column = "select rollno from 19_batch"
# column = get_particular_columns(connection, get_column)
# print(column)
# for rollno in column:
#      print(rollno)

# formulating the data using dataframe in pandas.
from_db = []
get_all_data = "select * from 19_batch"
data = get_all_records(connection, get_all_data)
for dt in data:
     from_db.append(dt)
columns = ["rollno", "name", "department"]
df = pd.DataFrame(from_db, columns=columns)
print(df)