'''
Author: TrungLC
Created Date: 16th Mar 2022
This script take overview of the database PG
Output to text file: /trunglc/git_workspace/python_test/output/db.txt
Format output file: total rows and size (in bytes) of 1 record stored in each line
'''
import sys
import psycopg2

#Region database configuration
DB_HOST = "localhost"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_PORT = 5432
#endregion

def connect_db():
    """
    Open global connection use for all requests.
    """
    global connection
    connection = psycopg2.connect(
        user = DB_USER,
        password = DB_PASSWORD,
        host = DB_HOST,
        port = DB_PORT,
        database = DB_NAME)

    connection.autocommit = True

def disconnect_db():
    """
    Disconect from the database
    """
    if connection:
        connection.close()

def count_table(table_name):
    count = 0
    try:
        cursor = connection.cursor()
        sql = "SELECT count(*) FROM {table_name}".format(table_name = table_name)
        cursor.execute(sql)
        fetch_records = cursor.fetchall()

        for row in fetch_records:
            count = row[0]

        return count
    except:
        print("Connect DB failed")
    finally:
        if connection:
            cursor.close()

def get_record_size(table_name):
    try:
        cursor = connection.cursor()
        sql = "SELECT * FROM {table_name} LIMIT 1".format(table_name = table_name)
        cursor.execute(sql)
        result = cursor.fetchone();
        return sys.getsizeof(result)
    except:
        print("Connect DB failed")
    finally:
        if connection:
            cursor.close()

connect_db()
count = count_table("Sharding")
print(count)
record_size = get_record_size("Sharding")
print(record_size)

output_file = "/trunglc/git_workspace/python_test/output/db.txt"
with open(output_file, 'w') as f:
    f.writelines([str(count) + "\n", str(record_size)])

disconnect_db()
