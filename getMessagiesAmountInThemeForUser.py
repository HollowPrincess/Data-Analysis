import pandas as pd

from queries.getTestsStarts import ret as problemStartsGenerator
from queries.getVideoStartAndEnd import ret as videoStartAndEndGenerator
from queries.getMessagiesCreationTime import ret as messagiesCreationGenerator

messagiesInfoDF=pd.DataFrame(messagiesCreationGenerator()).sort_values(by=['time'])
videoStartsDF=pd.DataFrame(videoStartAndEndGenerator()).sort_values(by=['time'])
problemStartsDF=pd.DataFrame(problemStartsGenerator()).sort_values(by=['time'])

videoStartsDF=videoStartsDF.groupby(by=['course_id','theme_id','user_id'])['time'].min()
videoStartsDF=videoStartsDF.reset_index()
problemStartsDF=problemStartsDF.groupby(by=['course_id','theme_id','user_id'])['time'].min()
problemStartsDF=problemStartsDF.reset_index()

startsDF=(pd.concat([videoStartsDF, problemStartsDF],ignore_index=True)).groupby(by=['course_id','theme_id','user_id'])['time'].min()
startsDF=startsDF.reset_index()
startsDF=startsDF.sort_values(by=['time'])

def getMessagiesAmount():
    users=startsDF['user_id'].unique()
    course_id=startsDF['course_id'].unique()
    messagiesArray=[]
    for user in users:
        startTime=False
        timeDF=startsDF.loc[startsDF['user_id']==user]
        for row in timeDF.iterrows():
            if startTime:
                startTime=endTime
                endTime=row[1]['time']
                messagiesNum=messagiesInfoDF.loc[messagiesInfoDF['user_id']==user].loc[messagiesInfoDF['time']>=startTime].loc[messagiesInfoDF['time']<endTime].shape[0]
                messagiesArray.append([course_id,theme,user,messagiesNum])
            else:
                startTime=row[1]['time']
                endTime=startTime
            theme=endTime=row[1]['theme_id']
        messagiesNum=messagiesInfoDF.loc[messagiesInfoDF['user_id']==user].loc[messagiesInfoDF['time']>=endTime].shape[0]
        messagiesArray.append([course_id,theme,user,messagiesNum])
    messagiesNumDF=pd.DataFrame(messagiesArray, columns=['course_id','theme_id','user_id','messagiesNum'])
    return messagiesNumDF