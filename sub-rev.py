import sys
import paho.mqtt.client as mqtt
import time
import requests
import json

import platform
version = platform.python_version().split(".")[0]
if version == "3":
  import app_config.app_config as cfg
elif version == "2":
  import app_config as cfg
config = cfg.getconfig()
topic="clientid/ingestid"

def publish():
    v=0
    for i in range(5):
        dict={}
        dict["t"] = (time.time()*1000)//1000*1000
        dict["v"]=v
        TOPIC=topic+"sensor"+str(i)
        client.publish(TOPIC,json.dumps(dict))
        v+=200

def on_message(client, userdata, msg):
    topic = msg.topic
    body = json.loads(msg.payload)
    # body = json.loads(msg.payload.decode('utf-8'))
    # print(body)
    client2.publish(msg.topic,msg.payload)


def on_message2(client, userdata, msg):
    print(msg.payload)
    body = json.loads(msg.payload.decode('utf-8'))
    print(body)

def on_connect2(client, userdata, flags, rc):
    #print ("rc of thermax", rc)
    pass#client.subscribe("#")

def on_connect(client, userdata, flags, rc):
    #print ("rc of node 5", rc)
    client.subscribe(topic)

def on_log(client, userdata, obj, buff):
    print("log: " + str(buff))
    pass

BROKER_ADDRESS = "40.114.36.172" #"10.38.40.253"#"122.185.21.154"#"0.0.0.0"http:///

client = mqtt.Client()
publish()
client.on_connect = on_connect
client.on_message = on_message
client.on_log = on_log

try:
    username = config["BROKER_USERNAME"]
    password = config["BROKER_PASSWORD"]
    client.username_pw_set(username=username, password=password)
except:
    pass

client.connect(BROKER_ADDRESS, 1883, 2800)

BROKER_ADDRESS = "10.211.19.37"#"10.38.40.253"#"122.185.21.154"#"0.0.0.0"http:///

client2 = mqtt.Client()
client2.on_connect = on_connect2
client2.on_message = on_message2
client2.on_log = on_log
try:
    username = config["BROKER_USERNAME"]
    password = config["BROKER_PASSWORD"]
    client2.username_pw_set(username=username, password=password)
except:
    pass

client2.connect(BROKER_ADDRESS, 1883, 2800)

client2.loop_start()
client.loop_forever()
