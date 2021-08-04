
import pandas as pd
import glob
import json
import datetime
import dateutil
import statistics
import os
import matplotlib.pyplot as plt
import numpy as np

class Sleep_Features:
    def computeFeatures(sleepDataWindows, sleepDf): # takes in dictionary of extracted sleep data and complete sleep data
        Dfs = list(sleepDf.groupby(sleepDf['dateTime'].dt.date))
        sleepDataFrame = [t[1] for t in Dfs]
        columns = ['dateTime', 'Sleep_Regularity', 'Duration']
        validDays = []
        user_dfs = list()
        user_rows = []

        for window in range(len(sleepDataWindows)):
            date = sleepDataWindows[window]['dateTime'].iloc[0].date()
            month = sleepDataWindows[window]['dateTime'].iloc[0].month
            # checks to see if first window is the first day of data
            if window < 1:
                sleepDataFrame[window]['dateTime'] = pd.to_datetime(sleepDataFrame[window]['dateTime'], format="%Y-%m-%d %H:%M:%S")
                validDays.append(sleepDataFrame[window])
                validDays.append(sleepDataWindows[window])
            else:
                
                if sleepDataWindows[window]['dateTime'].iloc[0].date() != validDays[len(validDays) - 1]['dateTime'].iloc[0].date():
                    if sleepDataWindows[window]['dateTime'].iloc[0].month == month:
                        if  len(validDays) > 7:
                            validDays.pop(0)
                        validDays.insert(len(validDays), sleepDataWindows[window])

                    else:
                        if (validDays[len(validDays)]['dateTime'] - validDays[(len(validDays) - 1)]['dateTime']).days != 1:
                            validDays.clear()
                        else:
                            if  len(validDays) > 7:
                                validDays.pop(0)
                            validDays.insert(len(validDays), sleepDataWindows[window])
            if len(validDays) > 0:
                SRI = Sleep_Features.computeRegularity(validDays)
            else:
                SRI = np.nan
            duration = sleepDataWindows[window]['sleepValue'].astype(int).sum()
            features = [date,SRI, duration]
            user_rows.append(features)
        user_dfs = pd.DataFrame(user_rows, columns=columns)
        return user_dfs

    def computeRegularity(days):
        period = []
        for day in days:
            day_list = [1] * 2880
            for moment in range(len(day)):
                day['dateTime'].iloc[moment]
                conv_time = (day['dateTime'].iloc[moment].hour * 60) + (day['dateTime'].iloc[moment].minute * 1) + (day['dateTime'].iloc[moment].second // 30)
                # placing -1's for times where user is asleep in 24 hour windows 
                if int(day['sleepValue'].iloc[moment]) > 0:
                    day_list[conv_time] = -1
            period.append(day_list)
            score = 0

        #sum for all days
        for day in range(len(period) - 1):
            #sum values 24 hours apart
            for minute in range(len(period[day])):
                    score += period[day][minute] * period[day + 1][minute]
        score /= ((len(days) * 24 * 60 * 2) - (24 * 60 * 2))
        score -= 0.5
        sri = score * 200
        return sri