# About Python File

 - Daily Session : main.py and main2.py files
 - Spark submit :

``` bash
$ docker exec -it etl_data-spark-master-1 spark-submit --jars <resource/MySQL JAR 파일명.jar> --master spark://spark-master:7077 jobs/main.py
``` 

``` bash
$ docker exec -it etl_data-spark-master-1 spark-submit --jars <resource/ElasticSearch JAR 파일명.jar> --master spark://spark-master:7077 jobs/main2.py
```

## what is main.py and main2.py file
```  
  - main.py : Daily로 실행되는 파일이며, SparkSession 이후 정제된 데이터를 MySQL에 저장합니다.
  - main2.py : Daily로 실행되는 파일이며, SparkSession 이후 정제된 데이터를 Elastic Search에 저장합니다.
```
## what is sub.py and sub2.py file
```  
  - sub.py : 특정 기간동안 병합된 데이터를 처리할 needs가 있을때 사용되는 파일입니다.
  
             인자값으로 병합된 데이터를 입력해주어야 합니다. 

             이후 SparkSession 을 진행하며 DataModel1을 MySQL에 저장합니다.

  - sub2.py : sub.py와 동일하며 DataModel1을 ElasticSearch에 저장합니다.

```
## what is base.py , filter.py and mses.py
```
  - base.py // filter.py : data_pipline 디렉토리의 get.py method를 통해 가져온 데이터를 정제합니다.
  - mses.py : MySQL 과 ElasticSearch에 저장되는 방식을 작성한 파일입니다.
```



