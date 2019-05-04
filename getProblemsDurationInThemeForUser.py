import numpy as np
import dateutil.parser
import time
import datetime
import pandas as pd

from queries.getTestsStarts import ret as getTestsStartsGenerator
from queries.getSubmissionsTime import ret as getTestsEndsGenerator
from queries.getSubTimeForOpenAssess import ret as getAssessEndsGenerator

ProblemStartsDF=pd.DataFrame(getTestsStartsGenerator()).sort_values(by=['time'])
#ProblemStartsDF.to_csv(path_or_buf='problems_starts_time.csv',index=False)

ProblemEndsDF=pd.DataFrame(getTestsEndsGenerator()).sort_values(by=['time'])
#ProblemEndsDF.to_csv(path_or_buf='problems_ends_time.csv',index=False)

AssessEndsDF=pd.DataFrame(getAssessEndsGenerator()).sort_values(by=['time'])
#AssessEndsDF.to_csv(path_or_buf='assess_ends_time.csv',index=False)

"""
ProblemStartsDF = pd.read_csv('problems_starts_time.csv', sep=',', encoding='utf-8')
ProblemEndsDF = pd.read_csv('problems_ends_time.csv', sep=',', encoding='utf-8')
AssessEndsDF = pd.read_csv('assess_ends_time.csv', sep=',', encoding='utf-8')
"""
problems=ProblemStartsDF['problem_id'].unique()
durationDF=[]

def getDuration():
    for problem in problems:
        tmpDf=ProblemStartsDF.loc[ProblemStartsDF['problem_id']==problem]
        users=tmpDf['user_id']
        theme_id=list(tmpDf['theme_id'])[0]
        course_id=list(tmpDf['course_id'])[0]
        problem_type=list(tmpDf['problem_type'])[0]
        time_problem_start=list(ProblemStartsDF.loc[ProblemStartsDF['problem_id']==problem].loc[ProblemStartsDF['user_id']==user]['time'])
        for user in users:
            if problem_type=='graded':
                time_problem_end=list(AssessEndsDF.loc[AssessEndsDF['problem_id']==problem].loc[AssessEndsDF['user_id']==user]['time'])           
            else:
                time_problem_end=list(ProblemEndsDF.loc[ProblemEndsDF['problem_id']==problem].loc[ProblemEndsDF['user_id']==user]['time'])
            time_problem_start.extend(time_problem_end)
            time_problem_start.sort()
            
            for endtime in time_problem_end:
                problem_duration=(dateutil.parser.parse(time_problem_start[time_problem_start.index(endtime)])
                                  -
                                  dateutil.parser.parse(time_problem_start[time_problem_start.index(endtime)-1])).total_seconds()
                if problem_duration<0:
                    problem_duration=0
                    
            durationDF.append([course_id,theme_id,problem_type,problem,problem_duration,user])
    durationDF=pd.DataFrame(durationDF,columns=['course_id','theme_id','problem_type','problem_id','problem_duration','user_id'])
    durationDF=durationDF.groupby(['course_id','theme_id','problem_type','user_id'])['problem_duration'].sum()
    durationDF=durationDF.reset_index()
    #durationDF.to_csv(path_or_buf='problems_in_theme_duration.csv',index=False)
    return durationDF
    
    