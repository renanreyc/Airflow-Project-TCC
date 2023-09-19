![Alt text](img/pipeline_flow.png)


# Introduction 
TODO: Give a short introduction of your project. Let this section explain the objectives or the motivation behind this project. 

# Getting Started
TODO: Guide users through getting your code up and running on their own system. In this section you can talk about:
1.	Installation process
2.	Software dependencies
3.	Latest releases
4.	API references

## Installation process

1. Create env folder
2. Install python libs
3. Start the enviroment
4. Configure Services
5. Deploy the DAGS in ariflow
6. Final Considerations


### 1. Create env folder
- WINDOWS: execute init.bat
- LINUX: execute init.sh

### 2. Install python libs

```bash
pip install -r requirements.txt
```

### 3. Start the enviroment

! Notice the docker-compose requires a docker composer *v2.15.0* or superior !

```bash
docker-compose up -d
```

### 4. Configure Services

Prior to execute the pipelines first you need to configure the datalake and the airflow.

First you need to verify the enviroment parameters for DEV and HOMOLOG setup (stored in in the notebooks folder):

- [Dev](notebooks/config-dev.ini)
- [Homolog](notebooks/config-homolog.ini)

Than execute the folowing notebooks. To start the jupyther go to notebook folder and start the jupyter and execute the following datalakes

```
cd notebooks/
jupyter notebook --ip 0.0.0.0 --port 8897
```

**4.1. Configure Datalake**

[Notebook](notebooks/configure_datalake.ipynb)

**Configure Airflow**

[Notebook](notebooks/configure_airflow.ipynb)

## 5. TODO

## 6. Final Considerations

All done now you can use the pipeline

To stop the enviroment execute the following command

```
docker-compose down
```

## Software dependencies

- Docker
- Docker Compose 2.15
- Python


# Build and Test
TODO: Describe and show how to build your code and run the tests. 

# Contribute
TODO: Explain how other users and developers can contribute to make your code better. 
