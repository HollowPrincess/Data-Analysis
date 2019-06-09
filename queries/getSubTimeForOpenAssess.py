from elasticsearch import Elasticsearch
import requests
from  elasticsearch.helpers import scan as scan

es = Elasticsearch([{"host": "localhost", "port": 9200}])

body={
    "_source": [
        "context.user_id",
        "context.course_id",
        "referer",
        "event.submitted_at",
        "event.answer.file_key"
    ],
    "query": {
        "bool":{
            "must":[                
                {"match_phrase":{
                    "event_type": "openassessmentblock.create_submission"
                }}        
            ]       
        }
    }
}

def ret():
    for hit in scan(es,
         index="online_courses_full", 
         query=body
        ):        
        if hit.get("_source").get("event") :
            if hit.get("_source").get("event").get("answer") is not None:
                yield {                    
                    "user_id":hit.get("_source").get("context").get("user_id"),
                    "course_id":hit.get("_source").get("context").get("course_id"),
                    "time":hit.get("_source").get("event").get("submitted_at"),
                    "theme_id":hit.get("_source").get("referer").split('courseware/')[-1].split('/')[1],
                    "problem_id":hit.get("_source").get("event").get("answer").get("file_key").split('openassessment+block@')[-1]
        }
            
        
        
