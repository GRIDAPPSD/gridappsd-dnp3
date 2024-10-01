import os
from gridappsd import GridAPPSD
import gridappsd.topics as topics

os.environ['GRIDAPPSD_USER'] = 'system'
os.environ['GRIDAPPSD_PASSWORD'] = 'manager'


gapps = GridAPPSD()

gapps.send(topic=topics.field_input_topic(),
           message={})

