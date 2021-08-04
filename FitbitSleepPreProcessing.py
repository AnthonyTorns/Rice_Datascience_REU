import pandas as pd
import glob
import json
import datetime
import dateutil
import statistics
import os
import matplotlib.pyplot as plt 

class PreProcess_Sleep:

  def mergeSleepData():
    
     # finding path to data folder
    storedDataPath = 'data/'
    mergedRoot = 'data/merged/'
    if not os.path.exists(mergedRoot):
        merged_path = os.mkdir(mergedRoot)
    else:
        merged_path = mergedRoot
    
    merged_files = list()
    userFolders = sorted([userFolder for userFolder in glob.glob(storedDataPath + '/**/**') if not userFolder.startswith('.') and 'merged' not in userFolder])
    data_per_dirs = [data_dir for data_dir in glob.glob(storedDataPath + '/**') if not data_dir.startswith('.') and 'merged' not in data_dir]  
    users = sorted([os.path.basename(os.path.normpath(user)) for user in userFolders])
    feature = 'sleep'
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
        path = merged_path + user + "/"
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
    merged_path = 'data/merged' # grabbing merged data path
    mergedUsers = sorted([mergedUser for mergedUser in glob.glob(merged_path + '/**') if not mergedUser.startswith('.') and 'merged' in mergedUser])
    
    # reordering user json data file by time stamp
    for mergedUser in mergedUsers:
      files = [feature_file for feature_file in glob.glob(mergedUser + '/*.json') if 'sleep' in feature_file]
      for file in files:
        with open(file, "r") as unsorted_file:
          json_file = json.load(unsorted_file)
          if 'sleep' in file:
            json_files = [file for file in json_file if len(file['sleep']) > 0]
            sorted_file = sorted(json_files, key = lambda i: i['sleep'][0]['dateOfSleep'])
        with open(file, "w") as unsorted_file:
          json.dump(sorted_file, unsorted_file)

  def toDataFrame():
    userSleepDfs = dict()
    altSleepDfs = dict()
    merged_path = 'data/merged'
    mergedUsers = sorted([mergedUser for mergedUser in glob.glob(merged_path + '/**') if not mergedUser.startswith('.') and 'merged' in mergedUser])
    sleeps = []
    # creating a daframe for each user with dateTime and fit bit value as columns
    for mergedUser in mergedUsers:
        files = [feature_file for feature_file in glob.glob(mergedUser + '/*.json') if 'sleep' in feature_file]
        user = os.path.basename(os.path.normpath(mergedUser))
        for file in files:
            sleep_dicts = []
            with open(file, "r") as raw_file:
                json_file = json.load(raw_file)
                if 'sleep' in file:
                    sleep_dicts = list()
                    for i in range(len(json_file)):
                    #print(len(json_file[i]['sleep']))
                        dailySleep = json_file[i]['sleep'][0]['minuteData']
                        if len(json_file[i]['sleep']) > 1:
                            j = 1
                            for j in range(len(json_file[i]['sleep'])):
                                dailySleep.extend(json_file[i]['sleep'][j]['minuteData'])
                        sleep_dicts.append(pd.DataFrame.from_dict(pd.io.json.json_normalize(sorted(dailySleep, key = lambda x: x['dateTime']))))
                    for i in range(len(sleep_dicts)):
                      date = json_file[i]['sleep'][0]['dateOfSleep']
                      sleep_dicts[i] = sleep_dicts[i].assign(dateTime = lambda x: date + ' ' + x['dateTime'])
                      sleep_dicts[i].rename(columns = {'value' : 'sleepValue'}, inplace = True)
                    if len(sleep_dicts) > 0:
                      sleepDf = pd.concat(sleep_dicts, ignore_index = True).drop_duplicates()
                      sleepDf.rename(columns = {'value' : 'sleepValue'}, inplace = True)
                      userSleepDfs.update({user :sleepDf})
    return userSleepDfs

  def extractData(data, startTime, endTime): # takes user dictionary of data, dictionary of starttime and dictionary of end time
    dfs = data.assign(dateTime = lambda x: (pd.to_datetime(data['dateTime'], format="%Y-%m-%d %H:%M:%S")))
    windows = []
    # extracts data in time windows places all exrtacted data into a list
    for i in range(len(startTime)):
      half = dfs[dfs['dateTime'] >= pd.to_datetime(startTime[i].replace(hour = 0, minute = 0, second = 0), format="%Y-%m-%d %H:%M:%S")]
      final = half[half['dateTime'] <= pd.to_datetime(endTime[i], format="%Y-%m-%d %H:%M:%S")]
      if len(final) > 0:
        windows.append(final)
        
    return windows