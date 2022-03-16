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

list_thread_threshold = [5e3, 1e4, 5e4, 1e5, 2e5, 3e5, 1e6, 2e6]
list_thread_threshold_title = ["Very Small", "Small", "Normal", "Medium", "High", "Large", "Very Large", "Huge"]

lambda_threshold = lambda x : min([list_thread_threshold.index(t) for t in list_thread_threshold if t >= x])
#print(lambda_threshold(2000))

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
df_summary["thread_cat"] = df_summary.apply(lambda x: lambda_threshold(x["threads"]), axis=1)
#writelog(df_summary.columns)

df_os = pd.read_csv(working_folder + "/output/os.csv")
writelog("df_os shape:", df_os.shape)
#writelog(df_os.columns)

df_time = pd.read_csv(working_folder + "/output/time.csv")
writelog("df_time shape:", df_time.shape)
df_time["thread_cat"] = df_time.apply(lambda x: lambda_threshold(x["threads"]), axis=1)
#writelog(df_time.columns)

#Visualization
sns.scatterplot(x = "threads", y = "avg", hue = "thread_cat", data = df_summary).set(title = "PG processing time by Parallel Threads", 
    xlabel = "Number of parallel threads",
    ylabel = "Average PG processing time (ms)")
plt.xticks(rotation = x_label_rotation)
plt.savefig(working_folder + "/graph/report1.png")
#plt.savefig(working_folder + "/graph/report1.png", dpi = 300)

plt.clf()

sns.scatterplot(x = "threads", y = "time", hue = "thread_cat", data = df_time).set(title = "PG processing time by Parallel Threads",
    xlabel = "Number of parallel threads",
    ylabel = "PG processing time (ms)")
plt.xticks(rotation = x_label_rotation)
plt.savefig(working_folder + "/graph/report2.png")

sns.displot(x = "time", data = df_time, bins = 100).set(title = "PG processing time by Parallel Threads",
    xlabel = "PG processing time (ms)")
plt.xlim(-10, 25000)
plt.xticks(rotation = x_label_rotation)
plt.savefig(working_folder + "/graph/histogram.png")

writelog("End the report")
