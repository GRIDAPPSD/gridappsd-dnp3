# GridAPPSD DNP3 service

## Purpose

The dnp3 service will convert CIM measurements points to dnp3 points to integrate GridAPPS-D and DNP3 based DMS(survalent/eterra) and send the commnad inputs from DMS to GridLAB-D for further simulation.

## Requirements

1. Docker ce version 17.12 or better.  You can install this via the docker_install_ubuntu.sh script.  (note for mint you will need to modify the file to work with xenial rather than ubuntu generically)

2. Please clone the repository <https://github.com/GRIDAPPSD/gridappsd-docker> (refered to as gridappsd-docker repository) next to this repository (they should both have the same parent folder, assumed to be ~/git in docker-compose.yml)

``` bash
~/git
├── gridappsd-docker
└── gridappsd-dnp3	

```
## Adding the port of the Master server to GridAPPS-D docker file 

To make a connection between GridAPPS-D and the master(DNP3 server) , the port number of the server has to be entered in the docker-compose.yml file. For example, here we are adding 20000.

cd ../gridappsd-docker
vi docker-compose.yml

Edit the line below -61616:61616 and add 20000:20000 as shown below. Save the file and rerun your container(./run.sh -t develop). 

``` bash
 gridappsd:
    image: gridappsd/gridappsd${GRIDAPPSD_TAG}
    ports:
      # Each of the following are port mappings from the host into the
      # container.  The first three are used by GridAPPS-D for the different
      # protocols.
      - 61613:61613
      - 61614:61614
      - 61616:61616
      - 20000:20000
  ```    
      
 To check if the port was added to gridappsd conatainer run docker ps -a and see if 20000 is present for the gridappsd container.


