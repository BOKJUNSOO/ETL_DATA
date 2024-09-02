## ETL_DATA with pyspark

## About this Project
  - Nexon Open Api에서 제공하는 3만등 이내 유저의 랭킹데이터를 수집합니다.
  - SparkSQL을 이용하여 데이터를 정제하며 3가지 데이터 모델을 생성합니다.
  - 생성된 데이터모델을 ElasticSearch 와 MySQL에 저장합니다.
  - ElasticSearch에 저장된 데이터는 Kibana를 이용해 데이터를 시각화합니다.

## About DataModel
  - DataModel1

|character_name|class            |character_level|increase_exp  |level_up_amount|date      |exp_rank|
|:-------------|:----------------|:-----|:-------------|:---|:-----------|:----|
|망고쟁이불독  |아크메이지(불,독)|289|19150294360727|0              |2024-09-02|1       |
|도비          |와일드헌터       |293|13689384208247|0              |2024-09-02|2       |
|비돌          |보우마스터       |289            |13611032169161|0              |2024-09-02|3       |
|권태현        |제로             |288            |13458372927730|1              |2024-09-02|4       |
|유통기간      |히어로           |289            |13328955802197|0              |2024-09-02|5       |
|휴당          |듀얼블레이더     |289            |13317025072408|0              |2024-09-02|6       |
|케인WWE챔프   |배틀메이지       |295            |13239637749399|0              |2024-09-02|7       |
|구이          |엔젤릭버스터     |287            |13224898052331|1              |2024-09-02|8       |
|쁘쁘          |아란             |289            |13035731411585|0              |2024-09-02|9       |
|버터          |나이트로드       |297            |12679718366176|0              |2024-09-02|10      |
  - DataModel2
  - DataModel3


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