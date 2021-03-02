

### Notebooks and DNP3 list.xlsx

The MappingExample.ipynb note book reads the "DNP3 list.xlsx" file and builds a json dictionary with the index information for each conversion type.
The device_ip_port_config_all.json is built by the ports.ipynb notebook.device

Lookup device ip and port information in device_ip_port_config_all.json file.
The file is built by the ports.ipynb notebook.device

The SOEHandler in the master.py will handle the indexes to CIM MRID mapping and conversion.
The master_main.py will start all the masters and collect the built CIM message from each master.SOEHandler and send to the FIM topic.


To test use the following.

```bash
cd test

python master_main.py 'test outstation 1'

python outstation_main.py 'test outstation 1'

```

TODO Alka
Complete integration of capbank in new_start_service and test with stand alone outstation_cmd or outstation_main.

Command line send cmd to RTU_7
```
(dnp3_env) gridappsd@gridappsd-VirtualBox:/media/sf_git_scada_read/gridappsd-dnp3/test$ python master_send_cmd.py 'RTU_7' 
```
