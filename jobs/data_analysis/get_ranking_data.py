import requests
import json
import time
from datetime import datetime
headers = {
    "x-nxopen-api-key":"",
    "User-agent": "Mozilla/5.0"
}

# set a target date (without daytime)
target_date = "2024-08"

for i in range(29,30): # set a target daytime
    if i % 2 == 0:
        time.sleep(30)
        mydata = []
    else:                
        mydata = []
    for j in range(1,151) : # set a target page range
        if j % 20 == 0: 
            time.sleep(15)
            url = f"https://open.api.nexon.com/maplestory/v1/ranking/overall?date={target_date}-{i}&world_name=%EC%97%98%EB%A6%AC%EC%8B%9C%EC%9B%80" + "&page=" + str(j)
            req = requests.get(url = url, headers=headers)
            data = req.json()
            mydata.append(data)

        else :
            url = f"https://open.api.nexon.com/maplestory/v1/ranking/overall?date={target_date}-{i}&world_name=%EC%97%98%EB%A6%AC%EC%8B%9C%EC%9B%80" + "&page=" + str(j)
            req = requests.get(url= url, headers= headers)
            data = req.json()
            mydata.append(data)

    # save a daytime data
    file_path = f'C:/Users/brian/Desktop/JUNSOO/Project/ETL_DATA/data/ranking_{target_date}-{i}.json'
    with open(file_path, "w", encoding= "UTF-8-SIG") as f:
        json.dump(mydata
                  ,f
                  ,ensure_ascii=False
                  ,indent='\t')