from elasticsearch import Elasticsearch
import requests
from  elasticsearch.helpers import scan as scan

es = Elasticsearch([{"host": "localhost", "port": 9200}])

body={
    "_source": [
        "context.user_id",
        "context.course_id",
        "time"
    ],
    "query": {
        "bool":{
            "should":[                
                {"wildcard":{
                    "event_type": "*response_created"
                }},
                {"wildcard":{
                    "event_type": "*/threads/create"
                }},
                {"wildcard":{
                    "event_type": "*comment.created"
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
        if hit.get("_source"):
            yield {                    
                "user_id":hit.get("_source").get("context").get("user_id"),
                "course_id":hit.get("_source").get("context").get("course_id"),
                "time":hit.get("_source").get("time")
        }