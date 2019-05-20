from elasticsearch import Elasticsearch
import requests
from  elasticsearch.helpers import scan as scan

es = Elasticsearch([{"host": "localhost", "port": 9200}])

body={
    "_source": [
        "event.submission_uuid",
        "event.parts"
    ],    
    "query": {
        "bool":{
            "should":[                
                {"wildcard":{
                    "event_type": "openassessmentblock.peer_assess"
                }},
                {"wildcard":{
                    "event_source": "openassessmentblock.staff_assess"
                }},
                {"wildcard":{
                    "event_source": "openassessmentblock.self_assess"
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
            info=hit.get("_source").get("event").get("parts")
            grade=0
            maxGrade=0
            for obj in info:
                maxGrade+=obj['criterion']['points_possible']
                grade+=obj['option']['points']
            yield {    
                "sub_id":hit.get("_source").get("event").get("submission_uuid"),
                "grade":grade,
                "maxGrade":maxGrade
            }
                   