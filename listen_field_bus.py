import os
import time

from gridappsd import GridAPPSD
import gridappsd.topics as topics

os.environ['GRIDAPPSD_USER'] = 'system'
os.environ['GRIDAPPSD_PASSWORD'] = 'manager'


gapps = GridAPPSD()

def on_message(header, message):
    print(f"Header:\n{header}\nMessage:\n{message}")

gapps.subscribe(topic=topics.field_input_topic(), callback=on_message)

print(f"Listening Topic: {topics.field_input_topic()}")
while True:
    try:
        time.sleep(0.1)
    except KeyboardInterrupt:
        gapps.disconnect()
        print("Shutting Down")
