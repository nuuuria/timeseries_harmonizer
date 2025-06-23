## Kubernetes configuration

This YAML files (`harmonizer.yaml`,  `harmonizer-1m.yaml`, `harmonizer-1w.yaml`. `harmonizer-1d.yaml`, `harmonizer-1h.yaml`, `harmonizer-15m.yaml`, `kpis.yaml`) defines the specifications for Kubernetes resources, 
which is used to schedule recurring tasks within a Kubernetes cluster. 
By configuring these resources, the Kubernetes cluster can handle scheduled data 
ingestion and processing tasks efficiently, ensuring the system remains up-to-date without
manual intervention.

In this configuration, the yaml files schedules CronJobs.

---
## Containers Defined

Each yaml file defines different resources

### File `harmonizer.yaml`
* name: harmonizer
* image: harmonizer_dexma_modbus
* command: `python3 launcher.py -t devices`
* scheduled: Every 15 minutes

### File `harmonizer-1m.yaml`
* Two containers defined

  * name: harmonizerv2-1m-starter
    * image: harmonizer
    * command: `python3 launcher_v2.py -l start -f P1M -n 4 -t icat.influx`
    * scheduled: Once a month
    
  * name: harmonizerv2-1m-consumer
    * image: harmonizer
    * command: `python3 launcher_v2.py -l processor -f P1M -n 4 -t icat.influx`
    * scheduled: Once a month


### File `harmonizer-1w.yaml`

* Two containers defined

  * name: harmonizerv2-1w-starter
    * image: harmonizer
    * command: `python3 launcher_v2.py -l start -f P1W -n 4 -t icat.influx`
    * scheduled: Once a week
    
  * name: harmonizerv2-1w-consumer
    * image: harmonizer
    * command: `python3 launcher_v2.py -l processor -f P1W -n 4 -t icat.influx`
    * scheduled: Once a week


### File `harmonizerv2-1d.yaml`
* Two containers defined

  * name: harmonizerv2-1d-starter
    * image: harmonizer
    * command: `python3 launcher_v2.py -l start -f P1D -n 4 -t icat.influx`
    * scheduled: Once a day
    
  * name: harmonizerv2-1d-consumer
    * image: harmonizer
    * command: `python3 launcher_v2.py -l processor -f P1D -n 4 -t icat.influx`
    * scheduled: Once a day


### File `harmonizerv2-1h.yaml`
* Two containers defined

  * name: harmonizerv2-1h-starter
    * image: harmonizer
    * command: `python3 launcher_v2.py -l start -f PT1H -n 10 -t icat.influx`
    * scheduled: Every hour
    
  * name: harmonizerv2-1h-consumer
    * image: harmonizer
    * command: `python3 launcher_v2.py -l processor -f PT1H -n 10 -t icat.influx`
    * scheduled: Every hour


### File `harmonizerv2-15m.yaml`

* Two containers defined

  * name: harmonizerv2-15min-starter
    * image: harmonizer
    * command: `python3 launcher_v2.py -l start -f PT15M -n 10 -t icat.influx`
    * scheduled: Every 15 minutes
    
  * name: harmonizerv2-15min-consumer
    * image: harmonizer
    * command: `python3 launcher_v2.py -l processor -f PT15M -n 10 -t icat.influx`
    * scheduled: Every 15 minutes


### File `kpis.yaml`
* name: kpis
* image: harmonizer_dexma_modbus
* command: `python3 launcher.py -t kpis`
* scheduled: Every 15 minutes