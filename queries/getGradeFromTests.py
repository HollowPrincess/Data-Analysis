from elasticsearch import Elasticsearch
import requests
from  elasticsearch.helpers import scan as scan

es = Elasticsearch([{"host": "localhost", "port": 9200}])

body={
    "_source": [
        "event.grade",
        "event.max_grade",
        "context.user_id",
        "event.problem_id"
    ],    
    "query": {
        "bool":{
            "should":[                
                {"match":{
                    "event_type": "problem_check"
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
            if hit.get("_source").get("event").get("grade") :
                yield {    
                    "user_id":hit.get("_source").get("context").get("user_id"),
                    "problem_id":hit.get("_source").get("event").get("problem_id").split('problem+block@')[-1],
                    "grade":hit.get("_source").get("event").get("grade"),
                    "maxGrade":hit.get("_source").get("event").get("max_grade"),
                }
                   