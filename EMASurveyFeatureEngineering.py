import pandas as pd
import glob
import json
import datetime
import dateutil
import statistics
import os
import matplotlib.pyplot as plt

class SurveyEngineering:
  
  def preProcess():
    path = "EMA_Random/CSV Files"
    surveyFiles = glob.glob(path + "/*.csv")
    listofFiles = []
    print(surveyFiles)
    
    for file in surveyFiles:
      # Finds EMA Survey files and extracts PANAS data
      df = pd.read_csv(file, usecols = [0, 10, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60], index_col=None, header=0).iloc[3:, :]
      df.rename(columns={df.columns[0]:'dateTime', df.columns[1]:'Participant', df.columns[2]:'PANAS_Lonely', df.columns[3]:'PANAS_Alert', df.columns[4]:'PANAS_Upset', df.columns[5]:'PANAS_Inspired', df.columns[6]:'PANAS_Afraid', df.columns[7]:'PANAS_Nervous', df.columns[8]:'PANAS_Enthusiastic', df.columns[9]:'PANAS_Excited', df.columns[10]:'PANAS_Scared', df.columns[11]:'PANAS_Determined', df.columns[12]:'PANAS_Depressed', df.columns[13]:'PANAS_Distressed'},inplace = True)
      listofFiles.append(df)
    dataset = pd.concat(listofFiles, axis=0, ignore_index=True) # mergess all into one dataframe
    dataset.dropna(inplace = True)
    dataset.sort_values(by = ['Participant', 'dateTime'], inplace = True) # reorder
    
    return dataset

  def extractData(dataset):
    # returns data values
    data = dataset.iloc[: , 2:].values
    for i in range(len(data)):
      for j in range(len(data[i])):
        data[i][j] = int(data[i][j])
    return data

  def reverseScore(data):
    # reverse scores positive attributes
    for i in range(0, len(data)):
      for j in range(0, len(data[i])):
          data[i][j] = int(data[i][j])
          if ((j == 1 or j == 3 or j == 6 or j == 7 or j == 9)):
              data[i][j] = abs(data[i][j] - 6)
    return data
  
  def ucla(data):
    #ucla computation
    scores = [sum(nums) for nums in data]
    return scores
              
  def panas(data):
    #PA and NA Scoring
    positive = []
    negative = []
    for i in range(0, len(data)):
      posColumn = []
      negColumn = []
      for j in range(1, len(data[i])):
          if ((j != 0 and j != 10)):
              if (j == 1 or j == 3 or j == 6 or j == 7 or j == 9):
                  posColumn.append(data[i][j])
              else:
                  negColumn.append(data[i][j])
      positive.append(posColumn)
      negative.append(negColumn)
    paScore = [statistics.mean(nums) for nums in positive]
    naScore = [statistics.mean(nums) for nums in negative]
    return [paScore, naScore]
  
  def lonely(data):
    # extracts lonely label for each user at each timestamp
    lonely = []
    for i in range(0, len(data)):
      if data[i][0] == 1:
        lonely.append(0)
      else:
        lonely.append(1) 
    return lonely

  def surveyDf(dataset, UCLA, PA, NA, Lonely):
    # creates df of survey features
     df = pd.DataFrame(data = dataset.iloc[ :, 0:2])
     df['UCLA_Score'] = pd.Series(UCLA, index=df.index)
     df['PA_Score'] = pd.Series(PA, index=df.index)
     df['NA_Score'] = pd.Series(NA, index=df.index)
     df['Lonely Score'] = pd.Series(Lonely, index=df.index)
     return df
  
  def findStartTime(userDf, start):
    #computes start time for feature calculation
    df = pd.to_datetime(userDf['dateTime'])
    times = []
    for row in df:
      startOfDay = row - datetime.timedelta(hours=start) #computes features from previous x hours
      #startOfDay = row.replace(hour = 0, minute = 0, second = 0)
      times.append(startOfDay)
    return times
