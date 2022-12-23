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

def on_log(client, userdata, obj, buff):
    print("log: " + str(buff))

def publish():
    while True:
        sensor=int(input("enter sensor count:"))
        body={}
        v=0
        for i in range(1,sensor+1):
            t=int(time.time())
            body["sensor"+str(i)+";"+str(t)]=v
            v=v+200
            # body={"sensor1;1644305374":0,"sensor2;1644305374":200,"sensor3;1644305374":400}
        print(body)
        client.publish(topic_line,json.dumps(body))
        time.sleep(10)



def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.connected_flag=True #set flag
        print("Connected to MQTT Broker!")
    else:
        print("Failed to connect, return code %d\n", rc)

# BROKER_ADDRESS = "0.0.0.0"
BROKER_ADDRESS = config["BROKER_ADDRESS"]

  
client = paho.Client()
client.on_log = on_log
try:
    username = config["BROKER_USERNAME"]
    password = config["BROKER_PASSWORD"]
    client.username_pw_set(username=username, password=password)
except:
    pass
client.connect(BROKER_ADDRESS, 1883, 60)
publish()
# sched= BlockingScheduler()

# sched.add_job(func=publish, trigger="interval", seconds=60,max_instances=1)

# sched.start()

# #client.publish(topic_line+dataTagId, json.dumps(body))
client.loop_forever(retry_first_connection=True)