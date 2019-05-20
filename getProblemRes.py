import pandas as pd

from queries.getGradeFromAssess import ret as getGrade
from queries.getUserIDinAssess import ret as getUser
from queries.getGradeFromTests import ret as getTestGrade


def grades():
    graded=pd.DataFrame(getGrade())
    users=pd.DataFrame(getUser())
    graded=graded.set_index('sub_id').join(users.set_index('sub_id'),how='inner')
    graded=graded.reset_index()
    graded=graded.groupby(by=['sub_id','user_id','problem_id'])['grade','maxGrade'].sum()
    graded=graded.reset_index()
    graded=graded.loc[:,['user_id','problem_id','grade','maxGrade']]
    
    ungraded=pd.DataFrame(getTestGrade())
    ungraded=ungraded.groupby(by=['user_id','problem_id'])['grade','maxGrade'].sum()
    ungraded=ungraded.reset_index()
    return graded,ungraded
    