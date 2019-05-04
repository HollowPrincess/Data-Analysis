from elasticsearch import Elasticsearch
import requests
from  elasticsearch.helpers import scan as scan

es = Elasticsearch([{"host": "localhost", "port": 9200}])

body={
    "_source": [
        "context.user_id",
        "context.course_id",
        "referer",
        "time",
        "event.submission",
        "event.attempts",
        "event.problem_id" 
    ],
    "query": {
        "bool":{
            "must":[                
                {"match_phrase":{
                    "event_type": "problem_check"
                }},
                {"match":{
                    "event_source": "server"
                }}           
            ],
            "must_not":[
                {"match_phrase":{
                    "event_type": "problem_check_fail"
                }}
            ]            
        }
    }
}

def ret():
    for hit in scan(es,
         index="online_courses", 
         query=body
        ):        
        if hit.get("_source").get("event") :
            if hit.get("_source").get("event").get("submission")!={}:
                yield {                    
                    "user_id":hit.get("_source").get("context").get("user_id"),
                    "course_id":hit.get("_source").get("context").get("course_id"),
                    "time":hit.get("_source").get("time"),
                    "attempt_num":hit.get("_source").get("event").get("attempts"),
                    "theme_id":hit.get("_source").get("referer").split('courseware/')[-1].split('/')[1],
                    "problem_id":hit.get("_source").get("event").get("problem_id").split('problem+block@')[-1]
        }
            
        
        
