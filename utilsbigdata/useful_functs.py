import numpy as np
import pandas as pd
import ast


def q1(x):
    return x.quantile(0.25)

def q3(x):
    return x.quantile(0.75)

def day_type(x):
    if (0<=x.weekday()<5):
        return 'L'
    elif (x.weekday()==5):
        return 'S'
    elif (x.weekday()==6):
        return 'D'


def median_absolute_deviation_y(ys):
    #MAD
    median_y = np.median(ys)
    median_absolute_deviation_y = np.median([np.abs(y - median_y) for y in ys])
    return median_absolute_deviation_y

def mean_absolute_deviation_y(ys):
    #MeanAD
    mean_y = np.mean(ys)
    mean_absolute_deviation_y = np.mean([np.abs(y - mean_y) for y in ys])
    return mean_absolute_deviation_y

def outliers_modified_z_score(y, median_y, mean_absolute_deviation_y, median_absolute_deviation_y, threshold = 3.5):
    if median_absolute_deviation_y != 0:
        modified_z_scores = 0.6745 * (y - median_y) / median_absolute_deviation_y
    elif mean_absolute_deviation_y != 0:
        modified_z_scores = (y - median_y)/(1.253314 * mean_absolute_deviation_y)
    else:
        return np.NaN
    if np.abs(modified_z_scores) > threshold:
        return 1
    else:
        return 0

def boolean_from_no_flow(hour,daytype,s):
    if type(s) == str:
        d = ast.literal_eval(s)
        list_of_hours = d.get(daytype)
        if list_of_hours != None:
            for t in list_of_hours:
                if ((hour >= pd.to_datetime(t[0]).time()) and (hour <= pd.to_datetime(t[1]).time())):
                    return False
            return True
        else: return True
    else: return True

def get_coords(x):
    y = ast.literal_eval(x)
    return y[0][1],y[0][0], y[len(y)-1][1], y[len(y)-1][0]

def lookup(s):
    dates = {date:pd.to_datetime(date, format='%Y-%m-%d %H:%M:%S') for date in s.unique()}
    return s.map(dates)