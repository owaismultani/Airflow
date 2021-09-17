# How to run Assignment Locally

```bash
docker-compose up airflow-init && sudo docker-compose up -d
```
*Note: This will start airflow and neo4j locally*

Now the DAG named **tribes.ai** can be seen on [Airflow](http://0.0.0.0:8080/) 


### [Airflow](http://0.0.0.0:8080/)

*User*: **airflow**

*password*: **airflow** 

### [Neo4j](http://0.0.0.0:7474/)

*User*: **neo4j**

*password*: **test** 

## CleanUp
To stop and delete containers, without deleting images:

```
sudo docker-compose down --volumes --remove-orphans
```

To stop and delete containers, delete volumes with database data and download images, run:

```bash
sudo docker-compose down --volumes --rmi all
```


## Regards
*Owais Multani* 

*+919589978666*