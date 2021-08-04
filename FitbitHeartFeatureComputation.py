import pandas as pd
import glob
import json
import datetime
import dateutil
import statistics
import os
import numpy as np

class Heart_Features:
  def computeFeatures(heartData): #takes in extracted data
      #calculates statisics, returns data frame
    dates = []
    columns = [ 'dateTime', 'Heart_Rate_Mean', 'Heart_Rate_Median', 'Heart_Rate_StDev']
    userFeatures = []
    user_rows = []
    for window in heartData:
        date = window['dateTime'].iloc[-1]
        avg = statistics.mean(window['heartValue'].values)
        median = statistics.median(window['heartValue'].values)
        if len(window['heartValue'].values) > 1:
            stdev = statistics.stdev(window['heartValue'].values)
        else: 
            stdev = np.nan
        features = [date, avg, median, stdev]
        user_rows.append(features)
    user_df = pd.DataFrame(user_rows, columns=columns)
    return user_df