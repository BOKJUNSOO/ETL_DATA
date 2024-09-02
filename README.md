## ETL_DATA with pyspark

## About this Project
  - Nexon Open Api에서 제공하는 3만등 이내 유저의 랭킹데이터를 수집합니다.
  - SparkSQL을 이용하여 데이터를 정제하며 3가지 데이터 모델을 생성합니다.
  - 생성된 데이터모델을 ElasticSearch 와 MySQL에 저장합니다.
  - ElasticSearch에 저장된 데이터는 Kibana를 이용해 데이터를 시각화합니다.

## Directory Structure

```
| MySQL
  |- Dockerfile for build MySQL
| data
  |- Nexon Open Api data goes here
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