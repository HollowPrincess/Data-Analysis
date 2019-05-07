import pandas as pd

from queries.getTestsStarts import ret as problemStartsGenerator
from queries.getVideoStartAndEnd import ret as videoStartAndEndGenerator


def getThemeStarts():
    videoStartsDF=pd.DataFrame(videoStartAndEndGenerator()).sort_values(by=['time'])
    problemStartsDF=pd.DataFrame(problemStartsGenerator()).sort_values(by=['time'])
    
    videoStartsDF=videoStartsDF.groupby(by=['course_id','theme_id','user_id'])['time'].min()
    videoStartsDF=videoStartsDF.reset_index()
    problemStartsDF=problemStartsDF.groupby(by=['course_id','theme_id','user_id'])['time'].min()
    problemStartsDF=problemStartsDF.reset_index()
    
    startsDF=(pd.concat([videoStartsDF, problemStartsDF],ignore_index=True)).groupby(by=['course_id','theme_id','user_id'])['time'].min()
    startsDF=startsDF.reset_index()
    startsDF=startsDF.sort_values(by=['time'])
    return startsDF