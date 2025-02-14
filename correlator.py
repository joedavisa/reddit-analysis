import sys
import csv
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import ruptures as rpt
from statsmodels.tsa.stattools import grangercausalitytests
from scipy.signal import find_peaks, correlate
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def normalize(rdata):
    # 3 values in list with dates as key
    # {DateTime : [Players, reviews, rtot, (pc+rev)]}
    max_pc = 0
    max_rev = 0
    max_rtot = 0
    max_steam = 0
    count = 0
    for row in rdata.values():
        if int(row[0]) > max_pc:
            max_pc = int(row[0])
        if int(row[1]) > max_rev:
            max_rev = int(row[1])
        if int(row[2]) > max_rtot:
            max_rtot = int(row[2])
        if int(row[3]) > max_steam:
            max_steam = int(row[3])
        count += 1
    for row in rdata.values():
        row[0] = round(row[0] * 100 / max_pc, 2)
        row[1] = round(row[1] * 100 / max_rev, 2)
        row[2] = round(row[2] * 100 / max_rtot, 2)
        row[3] = round(row[3] * 100 / max_steam, 2) #better way?
    return rdata


def get_normalized_dict(filename):
    real_filename = filename.replace('_collated.csv','')
    with open(filename,'r', encoding="utf-8") as csv_read_handle:
        # , open(real_filename + "_correlations.csv",'w', newline='') as csv_write_handle
        csv_reader = csv.DictReader(csv_read_handle)
        line_count = 0
        empty_line_count = 0
        csv_dict = {}
        # DateTime| Players| reviews| rcom| rsub| rtot
        for row in csv_reader:
            if row["Players"] == '':
                pc = 0
            else:
                pc = row["Players"]
            if row["reviews"] == '':
                rev = 0
            else:
                rev = row["reviews"]
            if row["rtot"] == '':
                rtot = 0
            else:
                rtot = row["rtot"]
            if int(pc) == 0 or int(rev) == 0 or int(rtot) == 0:
                empty_line_count += 1
            else:
                csv_dict[datetime.strptime(row["DateTime"],'%d/%m/%Y %H:%M').date()] = [int(pc), int(rev), int(rtot), int(rev)+int(pc)]            
                line_count += 1
        return normalize(csv_dict)


def get_pc_lists(data_dict):    
    pc = []
    for row in data_dict.keys():
        pc.append(data_dict[row][0])
    return pc
def get_rev_lists(data_dict):    
    rev = []
    for row in data_dict.keys():
        rev.append(data_dict[row][1])
    return rev
def get_rtot_lists(data_dict):    
    rtot = []
    for row in data_dict.keys():
        rtot.append(data_dict[row][2])
    return rtot
def get_steam_lists(data_dict):    
    steam = []
    for row in data_dict.keys():
        steam.append(data_dict[row][3])
    return steam


def plot_data(key_list, pc, rev, rtot, maxes):
    plt.plot(key_list, pc, label='Player count')
    plt.plot(key_list, rev, label='Reviews')
    plt.plot(key_list, rtot, label='Reddit activity')
    plt.plot(maxes[0][0], maxes[0][1], 'x')     #pc
    plt.plot(maxes[1][0], maxes[1][1], 'go')    #rev
    plt.plot(maxes[2][0], maxes[2][1], 'ro')    #rtot

    plt.legend()
    plt.xlabel("Dates")
    plt.ylabel("Normalized percentages")
    plt.title("Reddit, Reviews, PlayerCounts")
    plt.show()


def plot_steam_data(key_list, df, maxes, changepoints):
    #rpt.display(df, [], pelt)
    plt.plot(df['Date'].values, df['Steam'].values, label='Steam activity')
    plt.plot(df['Date'].values, df['RedditTotal'].values, label='Reddit activity')
    plt.plot(maxes[0][0], maxes[0][1], 'x')     #steam
    plt.plot(maxes[1][0], maxes[1][1], 'go')    #rtot
    for bkp in changepoints[:-1]:
        plt.axvline(df['Date'].values[bkp], color="k",linewidth=1,
                        linestyle="--",
                        alpha=1.0)

    plt.legend()
    plt.xlabel("Dates")
    plt.ylabel("Normalized percentages")
    plt.title("Reddit vs Steam")
    plt.show()


def get_max_peaks(series, min_dist):
    # scuffed af damn
    peaks = find_peaks(series,height=0, distance=min_dist)
    max_peak = np.argpartition(peaks[1]['peak_heights'], -10)[-10:]
    max_peak = list(map(lambda x:[peaks[0][x], peaks[1]['peak_heights'][x]], max_peak))
    max_peak = list(map(list, zip(*max_peak)))
    max_peak[0] = [key_list[i] for i in max_peak[0]]
    return max_peak


def grangers_causation_matrix(data, variables, test='ssr_chi2test', verbose=False):
    # https://www.machinelearningplus.com/time-series/granger-causality-test-in-python/
    """Check Granger Causality of all possible combinations of the Time series.
    The rows are the response variable, columns are predictors. The values in the table 
    are the P-Values. P-Values lesser than the significance level (0.05), implies 
    the Null Hypothesis that the coefficients of the corresponding past values is 
    zero, that is, the X does not cause Y can be rejected.

    data      : pandas dataframe containing the time series variables
    variables : list containing names of the time series variables.
    """
    maxlag = 12
    df = pd.DataFrame(np.zeros((len(variables), len(variables))), columns=variables, index=variables)
    for c in df.columns:
        for r in df.index:
            test_result = grangercausalitytests(data[[r, c]], maxlag=maxlag, verbose=False)
            p_values = [round(test_result[i+1][0][test][1],4) for i in range(maxlag)]
            if verbose: print(f'Y = {r}, X = {c}, P Values = {p_values}')
            min_p_value = np.min(p_values)
            df.loc[r, c] = min_p_value
    df.columns = [var + '_x' for var in variables]
    df.index = [var + '_y' for var in variables]
    return df


def get_changepoint_dates(key_list, changepoints):
    dates = []
    for point in changepoints[:-1]:
        dates.append(key_list[point])
    return dates


def get_correlations(df):
    gcm1 = grangers_causation_matrix(df.loc[:,['PlayerCount','Reviews']], variables=['PlayerCount','Reviews'])
    gcm2 = grangers_causation_matrix(df.loc[:,['Reviews','RedditTotal']], variables=['Reviews','RedditTotal'])
    gcm3 = grangers_causation_matrix(df.loc[:,['RedditTotal','PlayerCount']], variables=['RedditTotal','PlayerCount'])
    gcm4 = grangers_causation_matrix(df.loc[:,['RedditTotal','Steam']], variables=['RedditTotal','Steam'])

    print(gcm1)
    print(gcm2)
    print(gcm3)
    print(gcm4)    

    # print(f'pc and rev (>0.5 is strong):\n{np.corrcoef(pc,rev)}')
    # print(f'rev and rtot (>0.5 is strong):\n{np.corrcoef(rev, rtot)}')
    # print(f'rtot and pc (>0.5 is strong):\n{np.corrcoef(rtot, pc)}')
    # print(f'steam and rtot (>0.5 is strong):\n{np.corrcoef(steam,rtot)}')

    return gcm1, gcm2, gcm3, gcm4

def get_peaks(pc, rev,rtot, steam):
    pc_peaks = get_max_peaks(pc, 20)
    rev_peaks = get_max_peaks(rev, 20)
    rtot_peaks = get_max_peaks(rtot, 20)
    steam_peaks = get_max_peaks(steam, 20)
    return pc_peaks, rev_peaks, rtot_peaks, steam_peaks

def get_changepoints(df, steam_peaks, rtot_peaks):
    steam_df = df['Steam'].values
    algo = rpt.KernelCPD(kernel="rbf",params={"gamma": 1e-2},min_size=15,jump=1).fit(steam_df)
    #algo = rpt.Pelt(model="rbf",min_size=20,jump=1).fit(steam_df)
    #algo = rpt.Dynp(model="rbf",min_size=20,jump=5).fit(steam_df)
    changepoints = algo.predict(n_bkps=25)
    #changepoints = algo.predict(n_bkps=15)
    # print(changepoints)

    dates = get_changepoint_dates(key_list, changepoints)
    print(dates)
    return changepoints, dates


def find_relevant_reddit_submissions(dates):

    return None


if __name__ == "__main__":
    filename = sys.argv[1]

    # get data from file and return a dict of {DateTime : [Players, reviews, rtot, Steam]} (Steam = players+rev])
    data_dict = get_normalized_dict(filename)

    # get lists of playercount, reviews, reddit_activity_total, dates for easier plotting
    pc = get_pc_lists(data_dict)
    rev = get_rev_lists(data_dict)
    rtot = get_rtot_lists(data_dict)
    steam = get_steam_lists(data_dict)
    key_list = sorted(data_dict.keys())

    columns = ['Date', 'PlayerCount','Reviews','RedditTotal','Steam']
    data =[(i, j, k, l, m) for i, j, k, l, m in zip(key_list, pc, rev, rtot, steam)]    
    df = pd.DataFrame(data, columns=columns)

    # find peaks, get granger correlation matrix, get dates of changepoints
    gc1, gc2, gc3, gc4 = get_correlations(df)
    pc_peak, rev_peak, rtot_peak, steam_peak = get_peaks(pc, rev, rtot, steam)
    changepoints, dates = get_changepoints(df, steam_peak, rtot_peak)

    #plot_data(data_dict, pc,rev,rtot)
    #plot_data(key_list, pc, rev, rtot, [pc_peaks, rev_peaks, rtot_peaks])
    plot_steam_data(key_list, df, [steam_peak, rtot_peak], changepoints=changepoints)

    # go through reddit submissions 3 days around changepoints and find posts that could be info drops