# GridAPPSD DNP3 service

## Purpose

The dnp3 service will convert CIM measurements points to dnp3 points to integrate GridAPPS-D and DNP3 based DMS(survalent/eterra) and send the commnad inputs from DMS to GridLAB-D for further simulation.

## Requirements

1. Docker ce version 17.12 or better.  You can install this via the docker_install_ubuntu.sh script.  (note for mint you will need to modify the file to work with xenial rather than ubuntu generically)

2. Please clone the repository <https://github.com/GRIDAPPSD/gridappsd-docker> (refered to as gridappsd-docker repository) next to this repository (they should both have the same parent folder, assumed to be ~/git in docker-compose.yml)

```` bash
~/git
├── gridappsd-docker
└── gridappsd-dnp3	
```

## Adding the dnp3 to container

In order to add the dnp3 service to the container you will need to modify the docker-compose.yml file included in the gridappsd-docker repository.  Under the gridappsd service there is an example volumes leaf that is commented out.  Uncomment and modify these lines to add the path for the state estimator and conf file.  Adding these lines will mount the stat estimator on the container's filesystem when the container is started.

````
#    volumes:
#      - ~/git/gridappsd-state-estimator/state-estimator:/gridappsd/services/state-estimator
       - ~/git/gridappsd-state-estimator/state-estimator/state-estimator.config:/gridappsd/services/state-estimator.config

     volumes:
       - ~/git/gridappsd-dnp3/dnp3:/gridappsd/services/dnp3
       - ~/git/gridappsd-dnp3/dnp3/dnp3.config:/gridappsd/services/dnp3.config

````

