'''
Author: TrungLc
Created Date: 16th Mar, 2022
Load dataset from csv and predict something
'''
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

working_folder = "/trunglc/git_workspace/python_test"

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
sns.lineplot(x = "threads", y = "avg", data = df_summary).set(title = "PG processing time by Parallel Threads")
plt.xlabel("Number of parallel threads")
plt.ylabel("Average PG processing time")
plt.savefig(working_folder + "/graph/report1.png", dpi = 300)

writelog("End the report")
