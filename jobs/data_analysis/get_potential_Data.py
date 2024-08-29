# get data
import requests
import json
headers = {
    "x-nxopen-api-key": "My api key"
}

date = "2024-05-05" # my miracle time..
urlString = f"https://open.api.nexon.com/maplestory/v1/history/cube?count=1000&date={date}" 

response = requests.get(urlString, headers = headers)
myapi = response.json()

# save_path and file_name
file_path = f'C:/Users/brian/Desktop/JUNSOO/Project/Game_domain/data/myapi_c_{date}.json'

# save
with open(file_path,"w", encoding = "UTF-8-SIG") as f:
    json.dump(myapi,
              f,
              ensure_ascii=False,
              indent = '\t')
