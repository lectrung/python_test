# Author: TrungLC
# Email: lectrung@gmail.com
# Contact number: 0902712727
# Created Date: 15th Mar 2022
# Location: HCMC
# Version: 1

"""
Input: read the tuple from file /trunglc/git_workspace/python_test/input/config.txt
Output: The script will generate the output file /trunglc/git_workspace/python_test/output/result.csv including below columns

1. Number of parallel threads
2. Number of failed threads
3. Number of overload threads
4. Max time of processing data of threads
5. Min time of processing data of threads
7. Avg time of processing data of threads
8. CPU usage
9. Memory usage
10. Disk IO read
11. Disk IO write

File /trunglc/git_workspace/python_test/output/os.csv including columns
1. threads
2. cpu_count
3. cpu_percent
4. memory_percent
5. disk_io_read
6. disk_io_write

File /trunglc/git_workspace/python_test/output/time.csv including columns
1. threads
2. time

"""

import numpy as np
import os
import glob
import psutil
from datetime import datetime
import psycopg2
import random
import multiprocessing
import time
import threading

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

def get_os_environment():
    """
    Get some common OS metrics
    Return a dictionary
    """
    global cpu_count
    cpu_percent = psutil.cpu_percent(interval=0.5)
    memory_percent = psutil.virtual_memory().percent
    disk_io_read = psutil.disk_io_counters()[0]
    disk_io_write = psutil.disk_io_counters()[1]

    return {"cpu_count" : cpu_count, 
            "cpu_percent" : cpu_percent, 
            "memory_percent" : memory_percent, 
            "disk_io_read" : disk_io_read, 
            "disk_io_write" : disk_io_write}

def get_worse_environment(env1, env2):
    """
    Get the worse environment base on maximum metrics of 2 OS environment
    Parameters:
    env1 : dictionary
        Call from get_os_environment function
    env2 : dictionary
        Call from get_os_environment function
    RETURN : dictionary
    """
    result = {}

    for key in env1.keys():
        result[key] = max(env1.get(key), env2.get(key))

    return result

def merge_two_dicts(x, y):
    """Given two dictionaries, merge them into a new dict as a shallow copy."""
    z = x.copy()
    z.update(y)
    return z

class PG:
    """Store the list of Sharding Item base on URL"""
    def __init__(self, url):
        """
        Init class with a specific URL
        Parameters
        ----------
        url : str
            The URL of the User request
        """
        self.list = []
        self.error = 0
        try:
            dbStart = datetime.now()
            cursor = connection.cursor()
            sql = "SELECT * FROM sharding WHERE url_hash = '\\x{url}'".format(url = url)
            cursor.execute(sql)
            fetch_records = cursor.fetchall()

            for row in fetch_records:
                item = (row[0], row[1], row[2])
                self.list.append(item)
        except:
            self.error = 1
        finally:
            if connection:
                cursor.close()
            dbEnd = datetime.now()
            self.time = (dbEnd - dbStart).total_seconds() * 1000

def get_random_URL():
    """
    Get random distinct request URL from import folder
    """
    list = []
    #Choose the lucky imported file
    luckyIndex = random.randint(1, total_import_files)
    luckyFile = list_import_files[luckyIndex]

    print(luckyFile)
    with open(luckyFile) as f:
        contents = f.readlines()

    list = random.sample(contents, 1)

    return list

def check_db(url, index, n_threads):
    """
    Do the task 01 for a specific URL
    Parameters:
    ----------
    url : str
        The url of the request
    index : int
        The index of the Thread
    """
    global n_errors
    global n_overloads
    global min_time
    global max_time
    global cpu_threshold
    global memory_threshold
    global lambda_overload
    global worse_env
    global ratio_threshold
    global list_times
    global list_env

    random_number = random.randint(1, 100)

    #Set 100% - ratio_threshdold% percent to get the latest OS environment for better performance due to the huge number of requests
    if random_number > ratio_threshold:
        #Get server environment before connect to the PG database
        server_env = get_os_environment()
        list_env.append(server_env)
        worse_env = get_worse_environment(server_env, worse_env)

        #Check overload via lambda function
        if lambda_overload(server_env, cpu_threshold, memory_threshold):
            n_overloads += 1

    pg = PG(url)
    #if has error, increase the n_errors and maybe stop the iter
    if pg.error > 0:
        n_errors += n_errors

    if min_time > pg.time or min_time == 0:
        min_time = pg.time

    if max_time < pg.time:
        max_time = pg.time

    if pg.time > 0:
        list_times.append(pg.time)

def parse_tuple(string):
    try:
        s = eval(string)
        if type(s) == tuple:
            return s
        return
    except:
        return

def writelog(*args):
    '''
    Write log to file and screen
    Parameters:
    args : str
        The text message need to be logged
    '''
    log_file = "/trunglc/git_workspace/python_test/log/log.txt"
    print(*args)
    with open(log_file, 'a') as f:
        for x in args:
            f.write(str(x))
        f.write("\n")

def get_step(n):
    if n < 1e4:
        return 500

    if n < 1e5:
        return 5000

    if n < 2e5:
        return 8000

    if n < 3e5:
        return 15000

    return 30000

time_start = datetime.now()
writelog("Start the test ...")

cpu_count = os.cpu_count()

connect_db()
import_folder = "/home"
list_import_files = glob.glob(import_folder + "/*.csv")
total_import_files = len(list_import_files)

#Read tuple from file config
#The tuple with format (from_thread, to_thread, cpu_threshold, memory_threshold, ratio_threshold, summary_file, os_file, time_file)
config_file = "/trunglc/git_workspace/python_test/input/config.txt"
with open(config_file, "r") as f:
    config_str = f.read()
#writelog(config_str)
config_data = parse_tuple(config_str)
writelog("Run test script from: ", config_data[0], " to: ", config_data[1])
writelog("CPU threshold = ", config_data[2])
writelog("Memory thresold = ", config_data[3])

cpu_threshold = config_data[2]
memory_threshold = config_data[3]
ratio_threshold = config_data[4]

step = 0

list_thread_per_second = []
n_iter = config_data[0]

while True:
    list_thread_per_second.append(n_iter)
    step = get_step(n_iter)
    n_iter = n_iter + step
    if n_iter > config_data[1]:
        break

list_thread_per_second.append(n_iter)
#print(list_thread_per_second)
#print(len(list_thread_per_second))
#exit()

#This below code will choose a random csv from /home mountpoint and choose some random URL
#url = get_random_URL()
#I reboot the Server and I don't know why the /home mountpoint is lost. So I have to assign the test url manually
url = "18b0ba4e3980"
n_errors = 0
n_overloads = 0
min_time = 0
max_time = 0
avg_time = 0
list_times = []
list_env = []

lambda_overload = lambda s, c, m : True if (s.get('cpu_percent') > c or s.get('memory_percent') > m) else False

summary_file = config_data[5]
summary_header = "threads,errors,overloads,max,min,avg,cpu_count,cpu_percent,memory_percent,disk_io_read,disk_io_write"

with open(summary_file, 'w') as f:
    f.write(summary_header + "\n")

os_file = config_data[6]
os_header = "threads,cpu_count,cpu_percent,memory_percent,disk_io_read,disk_io_write"
with open(os_file, 'w') as f:
    f.write(os_header + "\n")

time_file = config_data[7]
time_header = "threads,time"
with open(time_file, 'w') as f:
    f.write(time_header + "\n")

for n_threads in list_thread_per_second:
    threads = []

    n_errors = 0
    n_overloads = 0
    
    min_time = 0
    max_time = 0
    avg_time = 0
    list_times = []
    list_env = []

    worse_env = get_os_environment()

    #Create thread that connect to PG
    for i in range(1, n_threads + 1):
        t = threading.Timer(1, check_db, [url, i, n_threads])
        threads.append(t)

    [t.start() for t in threads]
    [t.join() for t in threads]

    avg_time = np.mean(list_times)

    thread_dict = {"threads" : n_threads, "errors" : n_errors, "overloads" : n_overloads, "max" : max_time, "min" : min_time, "avg" : avg_time}
    d_merge = merge_two_dicts(thread_dict, worse_env)
    writelog(d_merge)

    with open(summary_file, 'a') as f:
        f.write(",".join([str(e) for e in d_merge.values()]) + "\n")

    with open(os_file, 'a') as f:
        for d in list_env:
            f.write(str(n_threads) + "," + ",".join([str(e) for e in d.values()]) + "\n")

    with open(time_file, 'a') as f:
        for time in list_times:
            f.write(str(n_threads) + "," + str(time) + "\n")

disconnect_db()

time_end = datetime.now()
total_duration = time_end - time_start

writelog("Max threads:", n_threads, " errors:", n_errors)
writelog("End the test. Time elapsed: ", total_duration.total_seconds() * 1000, " ms!")
