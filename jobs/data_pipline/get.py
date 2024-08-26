import requests
import json
import time
from datetime import datetime

# parameter
headers = {
    "x-nxopen-api-key" : "myapi",
    "User-agent" : "Mozilla/5.0"
}
target_date = datetime.now().strftime("%y-%m-%d")

# request url
url = f"https://open.api.nexon.com/maplestory/v1/ranking/overall?date={target_date}&world_name=%EC%97%98%EB%A6%AC%EC%8B%9C%EC%9B%80" + "&page=" + str(j)

# save path
file_path = f'C:/Users/brian/Desktop/JUNSOO/Project/Game_domain/data/ranking_{target_date}.json'

# request data from nexon_open_api without null
def get_data(target_date, url):
    mydata = []
    for i in range(1,151):
        if i % 20 == 0:
            time.sleep(15)
            req = requests.get(url = url, headers = headers)
            data = req.json()
            mydata.append(data)
        else :
            req = requests.get(url = url, headers = headers)
            data = req.json()
            mydata.append(data)
    # save a data with some "i"
    with open(file_path, "w", encoding= "UTF-8-SIG") as f:
        json.dump(mydata
                  ,f
                  ,ensure_ascii=False
                  ,indent='\t'
                  )
    return None

# weekly plan
def merge_data():
    weekly_data_list = []
    for i in range():       
        file_path = f"C:/Users/brian/Desktop/JUNSOO/Project/Game_domain/data/ranking_{target_date}.json"