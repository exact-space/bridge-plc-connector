import paho.mqtt.client as paho
import time
import sys
import json
import os
# import utils as ut
import pandas as pd
import app_config as cfg
# query = ts.timeseriesquery()
import requests
import logging
from logzero import logger
import platform
from apscheduler.schedulers.background import BlockingScheduler

clientId="6255083f0b6e76041e38"
ingestId="678909yhij6788"
topic_line = clientId+"/"+ingestId


bridge_topics = [topic_line+"/+","0","out"]

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


def on_log(client, userdata, obj, buff):
    print("log: " + str(buff))

def on_connect(client, obj, flags, rc):
    logger.info("Connecting to client:"+str(rc))
    #print("Connecting to client:", rc)
    TOPIC=topic_line+"/+"
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
        body = json.loads(msg.payload)
        topic = msg.topic
        logger.info(topic)
        logger.info(str(body))
        client2.publish(topic,json.dumps(body))
        logger.info("publishing to broker2 => CLIENT2")

def on_connect2(client, obj, flags, rc):
    logger.info("Connecting to client2:"+str(rc))
    #print("Connecting to client:", rc)
    # TOPIC=topic_line+"/+"
    # client.subscribe(TOPIC)


# BROKER_ADDRESS = "0.0.0.0"
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
# client.enable_bridge_mode()

BROKER_ADDRESS2 = "20.228.168.6"

print(BROKER_ADDRESS2)
print("Running port", port)
   
client2 = paho.Client()
client2.on_log = on_log
client2.on_connect = on_connect2
# client.on_message = on_message2
try:
    username = config["BROKER_USERNAME"]
    password = config["BROKER_PASSWORD"]
    client2.username_pw_set(username=username, password=password)
except:
    pass
client2.connect(BROKER_ADDRESS2, 1883, 60)

# client.loop_start()
client.loop_forever(retry_first_connection=True)