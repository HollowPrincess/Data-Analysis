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
            "must":              
                {"wildcard":{
                    "event_type": "*problem_show*"
                }}           
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