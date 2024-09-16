# ETL_DATA with pyspark


## 프로젝트 개요
Nexon Open API를 통해 제공된 데이터를 활용하여 엘리시움 서버의 개인 랭킹 데이터를 수집합니다. 

Python으로 작성된 SparkJob을 통해 정제 작업을 수행하고 5가지 데이터 모델을 생성합니다. 

생성된 데이터 모델은 ElasticSearch와 MySQL에 저장되며 Kibana를 통해 시각화합니다.


## 기술 스택
  - Nexon Open API : 데이터 소스
  - PySpark : 데이터 처리 및 정제
  - ElasticSearch : 데이터 저장 및 검색
  - MySQL : 데이터 저장
  - Kibana : 데이터 시각화


## 데이터 모델 설명


  ### 데이터 모델 1 : 직업별 지역 분포
이 모델은 직업별 "도원경" 이상의 유저 분포와 비율을 시각화 하는 것에 이용됩니다.


|key_value                   |Tallahart|Carcion|Arteria|Dowonkyung|sum |rank|date      |class            |
|:-------------------------|:------------|:------------|:------------|:------------|:------------|:------------|:-------------------|:------------|
|2024-09-09-아델             |53       |500    |1138   |832       |2523|1   |2024-09-09|아델             |
|2024-09-09-비숍             |91       |534    |1026   |725       |2376|2   |2024-09-09|비숍             |
|2024-09-09-나이트로드       |79       |471    |772    |513       |1835|3   |2024-09-09|나이트로드       |
|2024-09-09-듀얼블레이더     |63       |403    |750    |505       |1721|4   |2024-09-09|듀얼블레이더     |
|2024-09-09-섀도어           |31       |253    |575    |494       |1353|5   |2024-09-09|섀도어           |
|2024-09-09-아크메이지(불,독)|55       |349    |541    |385       |1330|6   |2024-09-09|아크메이지(불,독)|
|2024-09-09-윈드브레이커     |20       |221    |483    |384       |1108|7   |2024-09-09|윈드브레이커     |
|2024-09-09-소울마스터       |20       |164    |443    |438       |1065|8   |2024-09-09|소울마스터       |
|2024-09-09-아크메이지(썬,콜)|33       |244    |453    |324       |1054|9   |2024-09-09|아크메이지(썬,콜)|
|2024-09-09-패스파인더       |19       |220    |412    |326       |977 |10  |2024-09-09|패스파인더       |

***

  ### 데이터 모델 2 : 직업군별 경험치 획득량
이 모델은 가장 많은 경험치를 획득한 직업군을 찾아냅니다.

집계 전날의 레벨과 보유 경험치를 고려하여 도출된 결과로, 획득 경험치가 500,000,000,000 이상인 유저를 대상으로 집계됩니다. 

패치에 따른 직업군별 경험치 획득량의 양상을 예측할 때 유용합니다.

|class     |status |count|increase_exp_sum|increase_exp_max|increase_exp_avg     |date      |key_value                    |hunting_rank|
|:----------------------------|:--------------------|:------------|:------------|:------------|:------------|:------------|:-------------------|:------------|
|메카닉    |Arteria|14   |28304715181086  |6786306030892   |2.0217653700775715E12|2024-09-09|메카닉-Arteria-2024-09-09    |1           |
|나이트워커|Arteria|105  |194999826743771 |8053036690376   |1.8571412070835334E12|2024-09-09|나이트워커-Arteria-2024-09-09|2           |
|칼리      |Arteria|27   |50123579006474  |5266952296343   |1.8564288520916296E12|2024-09-09|칼리-Arteria-2024-09-09      |3           |
|일리움    |Arteria|9    |15373240640881  |3636051229345   |1.7081378489867778E12|2024-09-09|일리움-Arteria-2024-09-09    |4           |
|배틀메이지|Arteria|34   |56476299083697  |6370581425857   |1.6610676201087354E12|2024-09-09|배틀메이지-Arteria-2024-09-09|5           |
|팬텀      |Arteria|99   |163888924390808 |10054312357337  |1.6554436807152324E12|2024-09-09|팬텀-Arteria-2024-09-09      |6           |
|캡틴      |Arteria|29   |47763107212937  |6253501836118   |1.6470036969978276E12|2024-09-09|캡틴-Arteria-2024-09-09      |7           |
|제논      |Arteria|34   |55833831926342  |6332382614020   |1.642171527245353E12 |2024-09-09|제논-Arteria-2024-09-09      |8           |
|비숍      |Arteria|379  |620512229878666 |9492018178659   |1.637235435036058E12 |2024-09-09|비숍-Arteria-2024-09-09      |9           |
|보우마스터|Arteria|53   |86698559347474  |5655290702736   |1.6358218744806416E12|2024-09-09|보우마스터-Arteria-2024-09-09|10          |


***

  ### 데이터 모델 3 : 유저별 경험치 집계
이 모델은 유저별로 집계일 기준 레벨업에 필요한 경험치 양을 계산합니다. 

전날 대비 증가한 경험치 양을 통해 다음 레벨업까지 걸리는 기간을 예측할 수 있습니다.

경험치 증가량은 전날과 집계일 모두 존재하는 이름에 대해 집계한 결과입니다.
 
유저의 이탈률을 계산하는 데도 사용될 수 있습니다.

  **주요 속성:**
- **`need_exp_level_up`**: 집계일 기준 레벨과 보유 경험치의 차이를 이용해 도출
  > (음수로 표현된 수치의 유저들은 경험치 감량 패치 이후 접속을 하지 않은 유저입니다. 접속시 레벨업을 할 것으로 기대됩니다.)
- **`increase_exp`**: 집계일자와 전날의 데이터를 비교하여 증가한 경험치량을 도출
- **`need_day_level_up`**: 필요 경험치량과 증가한 경험치량을 이용해 다음 레벨업 일자를 예측


|character_name|class            |character_level|need_exp_level_up|increase_exp|date      |need_day_level_up|key_value              |
|:-------------|:----------------|:-----|:-------------|:-----------------|:------------------|:--------------------|:--------------------|
|승팔          |팔라딘           |280            |-4226031907569   |0           |2024-09-09|we need you T^T  |승팔-2024-09-09        |
|희라미수      |아델             |280            |-3458066432187   |0           |2024-09-09|we need you T^T  |희라미수-2024-09-09    |
|스페오디      |비숍             |278            |-2153235707178   |0           |2024-09-09|we need you T^T  |스페오디-2024-09-09    |
|레크타        |아크메이지(썬,콜)|276            |-1968251774425   |0           |2024-09-09|we need you T^T  |레크타-2024-09-09      |
|야수민석      |아델             |276            |-1955485424557   |0           |2024-09-09|we need you T^T  |야수민석-2024-09-09    |
|은서랴        |아델             |276            |-1679954619695   |0           |2024-09-09|we need you T^T  |은서랴-2024-09-09      |
|O대구스타일O  |아델             |279            |-1478275223603   |0           |2024-09-09|we need you T^T  |O대구스타일O-2024-09-09|
|타도연        |나이트로드       |277            |-1338082828865   |0           |2024-09-09|we need you T^T  |타도연-2024-09-09      |
|구멍난레깅스  |키네시스         |276            |-920907375495    |0           |2024-09-09|we need you T^T  |구멍난레깅스-2024-09-09|
|김다오니      |듀얼블레이더     |279            |-420853100096    |0           |2024-09-09|we need you T^T  |김다오니-2024-09-09    |


|character_name|class            |character_level|need_exp_level_up|increase_exp  |date      |need_day_level_up|key_value          |
|:-------------|:----------------|:-----|:-------------|:-----------------|:------------------|:--------------------|:--------------------|
|버터          |나이트로드       |297            |845602925603379  |17873103802689|2024-09-09|47.0             |버터-2024-09-09    |
|쁘쁘          |아란             |289            |35672002855018   |15657861826732|2024-09-09|2.0              |쁘쁘-2024-09-09    |
|벨님          |카인             |290            |294284107422718  |14487395763317|2024-09-09|20.0             |벨님-2024-09-09    |
|법행          |아크메이지(불,독)|294            |317266302378141  |13654591854001|2024-09-09|23.0             |법행-2024-09-09    |
|환타꺼        |비숍             |289            |44286884508017   |12354962593089|2024-09-09|4.0              |환타꺼-2024-09-09  |
|애뿔          |와일드헌터       |288            |132264311880226  |12269909625568|2024-09-09|11.0             |애뿔-2024-09-09    |
|사부          |바이퍼           |290            |8859485232819    |12198018091393|2024-09-09|Congratulation!  |사부-2024-09-09    |
|도비          |와일드헌터       |293            |197277110199205  |12154337680630|2024-09-09|16.0             |도비-2024-09-09    |
|때릴꺼영      |아델             |287            |52719167002490   |12104500969917|2024-09-09|4.0              |때릴꺼영-2024-09-09|
|웅솝o         |아크메이지(썬,콜)|288            |111678899679811  |12084700991687|2024-09-09|9.0              |웅솝o-2024-09-09   |


***

 ### 데이터 모델 4 : 자유전직 조회 테이블
모험가 직업군의 자유전직 여부를 조회할때의 성능을 위한 테이블 입니다.

- **for SQL**

|character_name|character_level|class            |date      |key_value          |
|:-------------|:----------------|:---------------------|:-------|:-----------|
|버터          |297            |나이트로드       |2024-09-09|버터-2024-09-09    |
|쁘쁘          |289            |아란             |2024-09-09|쁘쁘-2024-09-09    |
|벨님          |290            |카인             |2024-09-09|벨님-2024-09-09    |
|법행          |294            |아크메이지(불,독)|2024-09-09|법행-2024-09-09    |
|환타꺼        |289            |비숍             |2024-09-09|환타꺼-2024-09-09  |
|애뿔          |288            |와일드헌터       |2024-09-09|애뿔-2024-09-09    |
|사부          |290            |바이퍼           |2024-09-09|사부-2024-09-09    |
|도비          |293            |와일드헌터       |2024-09-09|도비-2024-09-09    |
|때릴꺼영      |287            |아델             |2024-09-09|때릴꺼영-2024-09-09|
|웅솝o         |288            |아크메이지(썬,콜)|2024-09-09|웅솝o-2024-09-09   |

***

 ### 데이터 모델 5 : 레벨 도달 유저 수
이 모델은 280, 285, 290과 같은 상위 지역 진입을 위한 레벨에 도달한 유저의 수를 도출합니다.

2024.08.22 기준 3만등 이내의 유저들은 모두 275레벨을 초과합니다. 


|class            |date      |status_change|count|
|:-------------|:----------------|:---------------------|:-------|
|팬텀             |2024-09-09|stay here    |734  |
|데몬슬레이어     |2024-09-09|stay here    |274  |
|섀도어           |2024-09-09|go Arteria!  |2    |
|보우마스터       |2024-09-09|stay here    |425  |
|플레임위자드     |2024-09-09|stay here    |242  |
|아크메이지(썬,콜)|2024-09-09|go Tallahart!|2    |
|은월             |2024-09-09|stay here    |525  |
|키네시스         |2024-09-09|go Carcion!  |1    |
|비숍             |2024-09-09|go Carcion!  |2    |
|신궁             |2024-09-09|stay here    |203  |
|나이트로드       |2024-09-09|go Arteria!  |3    |
|섀도어           |2024-09-09|go Tallahart!|1    |
|칼리             |2024-09-09|go Arteria!  |1    |
|히어로           |2024-09-09|stay here    |956  |
|아크메이지(불,독)|2024-09-09|go Carcion!  |2    |
|윈드브레이커     |2024-09-09|go Arteria!  |3    |
|배틀메이지       |2024-09-09|stay here    |265  |
|루미너스         |2024-09-09|stay here    |331  |
|데몬어벤져       |2024-09-09|stay here    |411  |
|키네시스         |2024-09-09|stay here    |214  |
|캡틴             |2024-09-09|stay here    |196  |
|나이트워커       |2024-09-09|go Arteria!  |2    |
|메르세데스       |2024-09-09|stay here    |558  |
|팔라딘           |2024-09-09|stay here    |589  |
|노블레스         |2024-09-09|stay here    |1    |
|칼리             |2024-09-09|stay here    |153  |
|비숍             |2024-09-09|go Arteria!  |3    |
|듀얼블레이더     |2024-09-09|go Arteria!  |2    |
|보우마스터       |2024-09-09|go Carcion!  |1    |
|소울마스터       |2024-09-09|go Carcion!  |1    |



## 디렉토리 구조

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

## 실행방법

1. **컨테이너 실행**:

``` bash
$ docker-compose up -d
```

2. **spark-submit**:

``` bash
$ docker exec -it etl_data-spark-master-1 spark-submit --jars <resource/jarsfile.jar> --master spark://spark-master:7077 jobs/main.py
```
