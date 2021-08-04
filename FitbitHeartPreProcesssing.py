import pandas as pd
import glob
import json
import datetime
import dateutil
import statistics
import os

class PreProcess_Heart:

  def mergeHeartData():

    # finding path to data folder
    storedDataPath = 'data/'
    mergedRoot = 'data/merged/'
    if not os.path.exists(mergedRoot):
        merged_path = os.mkdir(mergedRoot)
    else:
        merged_path = mergedRoot
    
    merged_files = list()
    userFolders = sorted([userFolder for userFolder in glob.glob(storedDataPath + '/**/**') if not userFolder.startswith('.') and 'merged' not in userFolder]) # Finidng all users in data folders
    users = sorted([os.path.basename(os.path.normpath(user)) for user in userFolders])
    feature = 'heart'
   
    userPaths = {}
    # creating list of all user paths for a particular user and mapping by ID
    for userFolder in userFolders:
      user = os.path.basename(os.path.normpath(userFolder))
      if user in userPaths:
        userPaths[user].append(userFolder)
      else:
        userPaths.update({user : []})
        userPaths[user].append(userFolder)

    # merging all data for each user into a sinlge file
    for user in userPaths:
        path = str(merged_path) + str(user) + "/"
        if not os.path.exists(path):
            os.mkdir(path)
        else:
            data_path = path
        files = []
        for userPath in userPaths[user]:
            for feature_file in glob.glob(userPath + '/' + feature + '/*.json'):
                with open(feature_file, 'r') as file:
                  files.append(json.load(file))
            merged_feature_file = merged_path + user + "/" + feature  + ".json"
            with open(merged_feature_file, 'w') as merged_file:
                json.dump(files, merged_file)
        
  def reOrder():
    merged_path = 'data/merged/' # grabbing merged data path
    mergedUsers = sorted([mergedUser for mergedUser in glob.glob(merged_path + '/**') if not mergedUser.startswith('.')])

    # reordering user json data file by time stamp
    for mergedUser in mergedUsers:
      files = [feature_file for feature_file in glob.glob(mergedUser + '/*.json') if 'heart' in feature_file]
      for file in files:
        with open(file, "r") as unsorted_file:
          json_file = json.load(unsorted_file)
          if 'heart' in file:
            sorted_file = sorted(json_file, key = lambda i: i['activities-heart'][0]['dateTime'])
        with open(file, "w") as unsorted_file:
          json.dump(sorted_file, unsorted_file)

  def removeEmpty():
    # grabing merged path and users in merged path
    merged_path = 'data/merged/'
    mergedUsers = sorted([mergedUser for mergedUser in glob.glob(merged_path + '/**') if not mergedUser.startswith('.')])

    #deleting all users in merged path that have empty time series data 
    for mergedUser in mergedUsers:
      files = [feature_file for feature_file in glob.glob(mergedUser + '/*.json') if 'heart' in feature_file]
      for file in files:
        with open(file, "r") as infile:
          json_file = json.load(infile)
          nonEmpty = [ f for f in json_file if len(f['activities-heart-intraday']['dataset']) > 0 ]
        with open(file, "w") as openfile:
          json.dump(nonEmpty, openfile)

  def toDataFrame():
    userHeartDfs = dict()
    merged_path = 'data/merged/'
    mergedUsers = sorted([mergedUser for mergedUser in glob.glob(merged_path + '/**') if not mergedUser.startswith('.')])
    hearts = []
    # creating a daframe for each user with dateTime and fit bit value as columns
    for mergedUser in mergedUsers:
        files = [feature_file for feature_file in glob.glob(mergedUser + '/*.json') if 'heart' in feature_file]
        user = os.path.basename(os.path.normpath(mergedUser))
        for file in files:
            heart_dicts = []
            with open(file, "r") as raw_file:
                json_file = json.load(raw_file)
                if 'heart' in file:
                    heart_dicts = [pd.DataFrame.from_dict(pd.io.json.json_normalize(json_file[i]['activities-heart-intraday']['dataset'])) for i in range(len(json_file)) if len(json_file[i]['activities-heart-intraday']['dataset']) > 0]
                    for i in range(len(heart_dicts)):
                      date = json_file[i]['activities-heart'][0]['dateTime']
                      heart_dicts[i] = heart_dicts[i].assign(time = lambda x: (date + ' ' + x['time']))
                      heart_dicts[i].rename(columns = {'time':'dateTime'}, inplace = True)
                    if(len(heart_dicts) > 0):
                      heartDf = pd.concat(heart_dicts, ignore_index=True).drop_duplicates()
                      heartDf.rename(columns = {'value':'heartValue'}, inplace= True)
                      userHeartDfs.update({user : heartDf}) # each data frame is mapped to a user
    return userHeartDfs

  def extractData(data, startTime, endTime): # takes user dictionary of data, dictionary of starttime and dictionary of end time
    dfs = data.assign(dateTime = lambda x: (pd.to_datetime(data['dateTime'], format="%Y-%m-%d %H:%M:%S")))
    windows = []
    # extracts data in time windows places all exrtacted data into a list
    for i in range(len(startTime)):
      half = dfs[dfs['dateTime'] >= pd.to_datetime(startTime[i], format="%Y-%m-%d %H:%M:%S")]
      final = half[half['dateTime'] <= pd.to_datetime(endTime[i], format="%Y-%m-%d %H:%M:%S")]
      if len(final) > 0:
        windows.append(final)
    return windows

