## ETL_DATA with pyspark


## Directory Structure

```
| data
  |- data with github archive
| jobs
  |- pyspark files go here
| resources
  |- .jars for spark third-party app(elastic Search and MySQL)
| docker-compose.yml
```

## How to run pyspark project

run containers:

``` bash
$ docker-compose up -d
```

spark-submit:

``` bash
$ docker exec -it etl_data-spark-master-1 spark-submit --jars <resource/jarsfile.jar> --master spark://spark-master:7077 jobs/main.py
```