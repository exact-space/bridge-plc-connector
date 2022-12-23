import paho.mqtt.client as paho
import time
import sys
import json
import os
# import utils as ut
import pandas as pd
import app_config as cfg
# query = ts.timeseriesquery()
import platform
from logzero import logger

# from apscheduler.schedulers.background import BlockingScheduler

# unitId=os.environ.get("UNIT_ID")
# if unitId == None:
#     print("No unit Id passed, exiting")
clientId="6255083f0b6e76041e38"
ingestId="678909yhij6788"
topic_line = clientId+"/"+ingestId    

version = platform.python_version().split(".")[0]
if version == "3":
  import app_config.app_config as cfg
elif version == "2":
  import app_config as cfg
config = cfg.getconfig()


port = os.environ.get("Q_PORT")
if not port:
    port = 1883
else:
    port = int(port)

# BROKER_ADDRESS = os.environ.get("BROKER_ADDRESS")
# if not BROKER_ADDRESS:

# print(BROKER_ADDRESS)
# print("Running port", port)



def on_log(client, userdata, obj, buff):
    print("log: " + str(buff))

def on_connect(client, obj, flags, rc):
    logger.info("Connecting to client:"+str(rc))
    #print("Connecting to client:", rc)
    client.subscribe(topic_line)

def on_message(client, userdata, msg):
        body = json.loads(msg.payload)
        topic = msg.topic
        # logger.info(topic)
        logger.info(str(body))
        for k,v in body.items():
            l=(k.split(";"))
            TOPIC=topic_line+"/"+l[0]
            dict={}
            dict["t"]=l[-1]
            dict["v"]=v
            client.publish(TOPIC,json.dumps(dict))
            # client2.publish(TOPIC,json.dumps(dict))


# def on_connect2(client, obj, flags, rc):
#     logger.info("Connecting to client2:"+str(rc))
#     #print("Connecting to client:", rc)  
#     client2.subscribe(topic_line)
          

# def on_message2(client, userdata, msg):
#         body = json.loads(msg.payload)
#         topic = msg.topic
#         # logger.info(topic)
#         logger.info(str(body))
#         for k,v in body.items():
#             l=(k.split(";"))
#             TOPIC=topic_line+"/"+l[0]
#             dict={}
#             dict["t"]=l[-1]
#             dict["v"]=v
#             client.publish(TOPIC,json.dumps(dict))

BROKER_ADDRESS = config["BROKER_ADDRESS"]

print(BROKER_ADDRESS)
print("Running port", port)
   
client = paho.Client()
client.on_log = on_log
client.on_connect = on_connect
client.on_message = on_message
try:
    username = config["BROKER_USERNAME"]
    password = config["BROKER_PASSWORD"]
    client.username_pw_set(username=username, password=password)
except:
    pass
client.connect(BROKER_ADDRESS, 1883, 60)

# BROKER_ADDRESS2 = config["BROKER_ADDRESS"]

# print(BROKER_ADDRESS2)
# print("Running port", port)
   
# client2 = paho.Client()
# # client.on_log = on_log2
# client2.on_connect = on_connect2
# client2.on_message = on_message2
# try:
#     username = config["BROKER_USERNAME"]
#     password = config["BROKER_PASSWORD"]
#     client2.username_pw_set(username=username, password=password)
# except:
#     pass
# client2.connect(BROKER_ADDRESS2, 1883, 60)

# #client.publish(topic_line+dataTagId, json.dumps(body))
# client.loop_start()
client.loop_forever(retry_first_connection=True)