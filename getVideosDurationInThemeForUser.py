import pandas as pd
import dateutil.parser
import time

from queries.getVideoStartAndEnd import ret as videoActionsGenerator
videoStateDF=pd.DataFrame(videoActionsGenerator()).sort_values(by=['time'])
#videoStateDF.to_csv(path_or_buf='video_actions_time.csv',index=False)
#videoStateDF = pd.read_csv('video_actions_time.csv', sep=',', encoding='utf-8')

def getDuration():
    users=videoStateDF['user_id'].unique()
    videoDurationDF=[]
    course_id=videoStateDF['course_id'][0]
    for user in users:
        start_time=False
        timeSequence=videoStateDF.loc[videoStateDF['user_id']==user].sort_values(by='time')
        video_id=0
        for row in timeSequence.iterrows():
            if row[1]['isPlaying']:
                if video_id!=row[1]['video_id']:
                    video_id=row[1]['video_id']
                    theme_id=row[1]['theme_id']
                start_time=row[1]['time']
            elif start_time:
                current_time=row[1]['time']
                duration=(dateutil.parser.parse(current_time)-dateutil.parser.parse(start_time)).total_seconds()
                videoDurationDF.append([course_id,theme_id,user,video_id,duration])
                start_time=False
    videoDurationDF=pd.DataFrame(videoDurationDF, columns=['course_id','theme_id','user_id','video_id','duration'])
    videoDurationDF=videoDurationDF.groupby(['course_id','theme_id','user_id'])['duration'].sum()
    videoDurationDF=videoDurationDF.reset_index()
    return videoDurationDF
                