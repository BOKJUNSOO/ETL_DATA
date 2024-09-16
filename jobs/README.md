# About Python File

 - Daily Session : main.py and main2.py files
 - Spark submit :

``` bash
$ docker exec -it etl_data-spark-master-1 spark-submit --jars <resource/MySQL JAR 파일명.jar> --master spark://spark-master:7077 jobs/main.py
``` 

``` bash
$ docker exec -it etl_data-spark-master-1 spark-submit --jars <resource/ElasticSearch JAR 파일명.jar> --master spark://spark-master:7077 jobs/main2.py
```

## shell
```  
  - spark-submit.sh : main.py 실행을 위한 shell 스크립트 입니다. 
  
  Daily로 실행되는 파일이며, SparkSession 이후 정제된 데이터를 MySQL에 저장합니다.
  - spark-submit2.sh : main.py 실행을 위한 shell 스크립트 입니다. 
  
  Daily로 실행되는 파일이며, SparkSession 이후 정제된 데이터를 Elastic Search에 저장합니다.
  - sub.sh : sub.py 실행을 위한 shell 스크립트 입니다.
```

## what is sub.py and sub2.py file

 - 특정 기간동안 병합된 데이터를 분석할 needs가 있을때 사용되는 파일입니다.

 - sub.py : 
```
 필요한 기간의 데이터를 일자별로 data 디렉토리에 위치시킵니다.
 인자값으로 저장하고 싶은 년도와 월, 일자의 범위를 입력합니다.
 ```

## what is base.py , filter.py and mses.py
```
  - base.py // filter.py : data_pipline 디렉토리의 get.py method를 통해 가져온 데이터를 정제합니다.
  - mses.py : MySQL 과 ElasticSearch에 저장되는 방식을 작성한 파일입니다.
```



