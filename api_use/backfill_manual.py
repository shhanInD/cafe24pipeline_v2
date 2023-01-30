## 수동으로 백필할 경우 돌리는 것
## 돌릴 때는 최신화된 connection 정보가 필요하므로, AWS에 있는 connectionInfo.json과 동기화 잘하고,
## 돌린 후에도 최신화 된 connectionInfo.json을 AWS로 업로드해야한다.
from utils import find_con_path, send_to_gbq, get_and_refresh_accesstoken
from customerprivacy import backfill
import json

con_path = find_con_path()
con_file_name = "connectionInfo_CustomerPrivacy"
with open(con_path+f"{con_file_name}.json", "r") as f:
    data = json.load(f)
auth_key = data["auth_key"]
json_file_path = con_path+"dbwisely-v2-01bfe15ef302.json"

acstok, asctok_expdt, reftok, reftok_expdt = get_and_refresh_accesstoken(auth_key, con_path, con_file_name)

startdate = "2023-01-24"
enddate = "2023-01-25"
cp_frontfill = backfill(startdate, enddate, acstok)
# cp_frontfill.to_excel("누락 시간 원본.xlsx", index = False)
print(cp_frontfill)
send_to_gbq(cp_frontfill,"HSH", "noorak_0124", json_file_path=json_file_path, if_exists="replace")