from elasticsearch import Elasticsearch
import requests
from  elasticsearch.helpers import scan as scan

es = Elasticsearch([{"host": "localhost", "port": 9200}])

body={
    "_source": [
        "event.submission_uuid",
        "event.answer.file_key",
        "context.user_id"
    ],    
    "query": {
        "bool":{
            "should":[                
                {"wildcard":{
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
            if hit.get("_source").get("event").get("answer"):
                yield {                    
                "sub_id":hit.get("_source").get("event").get("submission_uuid"),
                "user_id":hit.get("_source").get("context").get("user_id"),
                "problem_id":hit.get("_source").get("event").get("answer").get("file_key").split('openassessment+block@')[-1]
            }
                   
        