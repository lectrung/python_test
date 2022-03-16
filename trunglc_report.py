'''
Author: TrungLc
Created Date: 16th Mar, 2022
Load dataset from csv and predict something
'''
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

x_label_rotation = 15
working_folder = "/trunglc/git_workspace/python_test"

def convert_to_K_unit(number):
    '''
    Convert long number to 1K, 2K, 3K, etc....
    Parameters:
    number : int
        A positive number
    '''
    result = round(number / 1000)

    return str(result) + "K"

def writelog(*args):
    '''
    Write log to file and screen
    Parameters:
    args : str
        The text message need to be logged
    '''
    global working_folder
    log_file = working_folder + "/log/report.txt"
    print(*args)
    with open(log_file, 'a') as f:
        for x in args:
            f.write(str(x))
        f.write("\n")

writelog("Start the report")
#Load dataset from /python/output/*.csv
df_summary = pd.read_csv(working_folder + "/output/summary.csv")
writelog("df_summary shape:", df_summary.shape)
#writelog(df_summary.columns)

df_os = pd.read_csv(working_folder + "/output/os.csv")
writelog("df_os shape:", df_os.shape)
#writelog(df_os.columns)

df_time = pd.read_csv(working_folder + "/output/time.csv")
writelog("df_time shape:", df_time.shape)
#writelog(df_time.columns)

#Visualization
sns.scatterplot(x = "threads", y = "avg", data = df_summary).set(title = "PG processing time by Parallel Threads", 
    xlabel = "Number of parallel threads",
    ylabel = "Average PG processing time (ms)")
plt.xticks(rotation = x_label_rotation)
plt.savefig(working_folder + "/graph/report1.png")
#plt.savefig(working_folder + "/graph/report1.png", dpi = 300)

plt.clf()

sns.scatterplot(x = "threads", y = "time", data = df_time).set(title = "PG processing time by Parallel Threads",
    xlabel = "Number of parallel threads",
    ylabel = "PG processing time (ms)")
plt.xticks(rotation = x_label_rotation)
plt.savefig(working_folder + "/graph/report2.png")

writelog("End the report")
