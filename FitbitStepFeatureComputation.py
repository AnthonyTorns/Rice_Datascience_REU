import pandas as pd
import glob
import json
import datetime
import dateutil
import statistics
import os
import matplotlib.pyplot as plt 
import numpy as np
import math
import functools
import numpy as np

class Step_Features:
  def computeFeatures(stepData, sleepData, startTimes, endTimes): # takes in extracted user step and sleep dat , and start and end times for a user
    columns = ['dateTime' , 'Total_Steps', 'Step_Mean', 'Step_Median', 'Step_StDev']
    userFeatures = []
    user_rows = []
    step = pd.concat(stepData, ignore_index = True)  
    sleep = pd.concat(sleepData, ignore_index = True)  
    data = functools.reduce(lambda  left,right: pd.merge(left,right,
                                            how= 'outer'), [sleep, step])
    # next two lines to remove step data where user is asleep
    data = data[data['sleepValue'].isnull()] 
    data.drop_duplicates(inplace = True)

    for i in range(len(startTimes)):
        date = (pd.to_datetime(endTimes[i], format="%Y-%m-%d %H:%M:%S"))
        half = data[data['dateTime'] >= pd.to_datetime(startTimes[i], format="%Y-%m-%d %H:%M:%S")]
        final = half[half['dateTime'] <= pd.to_datetime(endTimes[i], format="%Y-%m-%d %H:%M:%S")]
        total = sum(final['stepValue'].values)
        if(len(final['stepValue'].values) > 0):
            avg = statistics.mean(final['stepValue'].values)
            median = statistics.median(final['stepValue'].values)
            if len(final['stepValue'].values) > 1:
                stdev = statistics.stdev(final['stepValue'].values)
            else: 
                stdev = np.nan
        else:
            avg = np.nan
            median = np.nan
            stdev = np.nan

        features = [date, total, avg, median, stdev]
        user_rows.append(features)
    user_df = pd.DataFrame(user_rows, columns=columns)
    return user_df
    
  def activeEntropy(stepData, sleepData, startTimes, endTimes): # takes in extracted user step and sleep dat , and start and end times for a user
    step = pd.concat(stepData, ignore_index = True)  
    sleep = pd.concat(sleepData, ignore_index = True)  
    data = functools.reduce(lambda  left,right: pd.merge(left,right,
                                            how= 'outer'), [sleep, step])
    entropies = []
    dates = []
    # removing data where user is sleep
    data = data[data['sleepValue'].isnull()]
    data.drop_duplicates(inplace=True)

    for i in range(len(startTimes)):
        dates.append((pd.to_datetime(endTimes[i], format="%Y-%m-%d %H:%M:%S")))
        half = data[data['dateTime'] >= pd.to_datetime(startTimes[i], format="%Y-%m-%d %H:%M:%S")]
        final = half[half['dateTime'] <= pd.to_datetime(endTimes[i], format="%Y-%m-%d %H:%M:%S")]
        values = np.array(final['stepValue'])
        activeTimes = np.count_nonzero(values)
        if len(values) > 0:
            prob = activeTimes/len(values)
            if prob > 0:
                activeE = ((prob * -1) * math.log(prob, 2))
            else:
                activeE = 0
        else:
            activeE = 0
        entropies.append(activeE)
        Active = pd.DataFrame({'dateTime' : dates , 'Active_Entropy' : entropies}).sort_values('dateTime')
    return Active
    
  def inactiveEntropy(stepData, sleepData, startTimes, endTimes): # takes in extracted user step and sleep dat , and start and end times for a user
      step = pd.concat(stepData, ignore_index = True)  
      sleep = pd.concat(sleepData, ignore_index = True)  
      data = functools.reduce(lambda  left,right: pd.merge(left,right, how= 'outer'), [sleep, step])

      # removing data where user is sleep
      data = data[data['sleepValue'].isnull()]
      entropies = []
      dates = []
      for i in range(len(startTimes)):
        dates.append((pd.to_datetime(endTimes[i], format="%Y-%m-%d %H:%M:%S")))
        half = data[data['dateTime'] >= pd.to_datetime(startTimes[i], format="%Y-%m-%d %H:%M:%S")]
        final = half[half['dateTime'] <= pd.to_datetime(endTimes[i], format="%Y-%m-%d %H:%M:%S")]
        values = np.array(final['stepValue'])
        inactiveTimes = len(values) - np.count_nonzero(values)
        if len(values) > 0:
            prob = inactiveTimes/len(values)
            if prob > 0:
                inactiveE = ((prob * -1) * math.log(prob, 2))
            else:
                inactiveE = 0
        else:
            inactiveE = 0
        entropies.append(inactiveE)
      Inactive = pd.DataFrame({'dateTime' : dates , 'Inactive_Entropy' : entropies}).sort_values('dateTime')
      return Inactive

