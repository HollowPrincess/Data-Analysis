import pandas as pd

from getMessagiesAmountInThemeForUser import getMessagiesAmount
from getVideosDurationInThemeForUser import getDuration as getVideoDuration
from getProblemsDurationInThemeForUser import getDuration  as getProblemDuration

def getModel():
    videoDurationDF=getVideoDuration()
    messagiesNum=getMessagiesAmount()
    problemsDurationDF=getProblemDuration()
    
    gradedProbDur=problemsDurationDF.loc[problemsDurationDF['problem_type']=='graded'].drop(['problem_type'], axis=1)
    ungradedProbDur=problemsDurationDF.loc[problemsDurationDF['problem_type']=='ungraded'].drop(['problem_type'], axis=1)
    userModel=gradedProbDur.join(ungradedProbDur.set_index(['course_id','theme_id','user_id']), on=['course_id','theme_id','user_id'],how='outer', lsuffix='_graded', rsuffix='_ungraded')
    userModel.reset_index()
    
    userModel=userModel.join(messagiesNum.set_index(['course_id','theme_id','user_id']), on=['course_id','theme_id','user_id'],how='outer')
    userModel.reset_index()
    
    userModel=userModel.join(videoDurationDF.set_index(['course_id','theme_id','user_id']), on=['course_id','theme_id','user_id'],how='outer')
    userModel.reset_index()
    
    userModel=userModel.fillna(0)
    userModel=userModel.rename(index=str, columns={"duration": "video_duration"})
    
    return userModel