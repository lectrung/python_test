'''
Author: TrungLc
Created Date: 16th Mar, 2022
Load dataset from csv and predict something
'''
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

x_label_rotation = 15
working_folder = "/trunglc/data"
graph_folder = "/trunglc/git_workspace/python_test/graph"

list_thread_threshold = [5e3, 1e4, 5e4, 1e5, 2e5, 3e5, 4e5, 4e6]
list_thread_threshold_title = ["<5K", "5K-10K", "10K-50K", "50K-100K", "100K-200K", "200K-300K", "300K-400K", ">400K"]

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
    log_file = working_folder + "/report.txt"
    print(*args)
    with open(log_file, 'a') as f:
        for x in args:
            f.write(str(x))
        f.write("\n")

cpu_threshold = 80
memory_threshold = 80
thread_cat_title = "No of Threads"

writelog("Start the report")
#Load dataset from /python/output/*.csv
df_summary = pd.read_csv(working_folder + "/summary.csv")
writelog("df_summary shape:", df_summary.shape)
df_summary["thread_cat"] = df_summary.apply(lambda x: lambda_threshold(x["threads"]), axis=1)
df_summary["p_error"] = round(df_summary["errors"] * 100 / df_summary["threads"])
df_summary["p_overload"] = round(df_summary["overloads"] * 100 / df_summary["threads"])
#writelog(df_summary.columns)

df_os = pd.read_csv(working_folder + "/os.csv")
writelog("df_os shape:", df_os.shape)
#writelog(df_os.columns)

df_time = pd.read_csv(working_folder + "/time.csv")
writelog("df_time shape:", df_time.shape)
df_time["thread_cat"] = df_time.apply(lambda x: lambda_threshold(x["threads"]), axis=1)
#writelog(df_time.columns)

#Visualization
sns.scatterplot(x = "threads", y = "avg", hue = "thread_cat", data = df_summary).set(title = "PG processing time by Parallel Threads", 
    xlabel = "Number of parallel threads",
    ylabel = "Average PG processing time (ms)")
plt.legend(title = thread_cat_title, loc = 'upper left', labels = list_thread_threshold_title)
plt.xticks(rotation = x_label_rotation)
plt.savefig(graph_folder + "/line.png")
#plt.savefig(working_folder + "/graph/report1.png", dpi = 300)

plt.clf()

sns.scatterplot(x = "threads", y = "p_error", hue = "thread_cat", data = df_summary).set(title = "Error percent by Parallel Threads",
    xlabel = "Number of parallel threads",
    ylabel = "Error percent")
plt.legend(title = 'No of threads', loc = 'upper left', labels = list_thread_threshold_title)
plt.xticks(rotation = x_label_rotation)
plt.savefig(graph_folder + "/error.png")

plt.clf()

sns.scatterplot(x = "threads", y = "p_overload", hue = "thread_cat", data = df_summary).set(title = "Overload percent by Parallel Threads",
            xlabel = "Number of parallel threads",
                ylabel = "Overload percent")
plt.legend(title = thread_cat_title, loc = 'upper left', labels = list_thread_threshold_title)
plt.xticks(rotation = x_label_rotation)
plt.savefig(graph_folder + "/overload.png")

plt.clf()

sns.scatterplot(x = "threads", y = "min", hue = "thread_cat", data = df_summary).set(title = "Min time (ms) by Parallel Threads",
    xlabel = "Number of parallel threads",
    ylabel = "Min time (ms)")
plt.legend(title = thread_cat_title, loc = 'upper left', labels = list_thread_threshold_title)
plt.xticks(rotation = x_label_rotation)
plt.savefig(graph_folder + "/min.png")

plt.clf()

sns.scatterplot(x = "threads", y = "max", hue = "thread_cat", data = df_summary).set(title = "Max time (ms) by Parallel Threads",
    xlabel = "Number of parallel threads",
    ylabel = "Max time (ms)")
plt.legend(title = thread_cat_title, loc = 'upper left', labels = list_thread_threshold_title)
plt.xticks(rotation = x_label_rotation)
plt.savefig(graph_folder + "/max.png")

plt.clf()

sns.boxplot(x = "thread_cat", y = "time", data = df_time).set(title = "PG processing time by Parallel Threads",
    xlabel = "Number of parallel threads",
    ylabel = "PG processing time (ms)")
plt.legend(title = thread_cat_title, loc = 'upper left', labels = list_thread_threshold_title)
plt.xticks(rotation = x_label_rotation)
plt.savefig(graph_folder + "/boxplot.png")

plt.clf()

sns.boxplot(y = "time", data = df_time).set(title = "PG processing time by Parallel Threads",
    ylabel = "PG processing time (ms)")
plt.savefig(graph_folder + "/time_boxplot.png")

plt.clf()

sns.displot(x = "time", data = df_time, bins = 200).set(title = "PG processing time by Parallel Threads",
    xlabel = "PG processing time (ms)")
plt.xlim(-10, 25000)
plt.xticks(rotation = x_label_rotation)
plt.savefig(graph_folder + "/time_histogram.png")

plt.clf()

sns.displot(x = "cpu_percent", data = df_os, bins = 200).set(title = "CPU Usage")
plt.axvline(cpu_threshold, color='red')
plt.savefig(graph_folder + "/cpu_histogram.png")

plt.clf()
sns.displot(x = "memory_percent", data = df_os, bins = 200).set(title = "Memory Usage")
plt.axvline(memory_threshold, color='red')
plt.savefig(graph_folder + "/memory_histogram.png")

plt.clf()

sns.displot(x = "disk_io_read", data = df_os, bins = 200).set(title = "Disk IO Read")
plt.savefig(graph_folder + "/io_read_histogram.png")

plt.clf()

sns.displot(x = "disk_io_write", data = df_os, bins = 200).set(title = "Disk IO Write")
plt.savefig(graph_folder + "/io_write_histogram.png")

writelog("End the report")
