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
        "event_type",
        "str_event" 
    ],
    "query": {
        "bool":{
            "should":[                
                {"match_phrase":{
                    "event_type": "play_video"
                }},
                {"match_phrase":{
                    "event_type": "pause_video"
                }},
                {"match_phrase":{
                    "event_type": "stop_video"
                }},
                {"match_phrase":{
                    "event_type": "page_close"
                }},
                {"match_phrase":{
                    "event_type": "seq_goto"
                }},
                {"match_phrase":{
                    "event_type": "seq_prev"
                }},
                {"match_phrase":{
                    "event_type": "seq_next"
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
        if hit.get("_source").get("str_event") :
            isPlaying=hit.get("_source").get("event_type")
            if isPlaying=="play_video":
                isPlaying=True
                video_id=hit.get("_source").get("str_event").split('id\": \"')[-1].split('\"')[0]
            else:
                isPlaying=False
                video_id=''
            
            yield {                    
                "user_id":hit.get("_source").get("context").get("user_id"),                    
                "course_id":hit.get("_source").get("context").get("course_id"),
                "time":hit.get("_source").get("time"),
                "theme_id":hit.get("_source").get("referer").split('courseware/')[-1].split('/')[1],
                "isPlaying":isPlaying,
                "video_id":video_id
            }