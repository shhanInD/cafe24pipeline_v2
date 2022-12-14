## 수동으로 백필할 경우 돌리는 것
## 돌릴 때는 최신화된 connection 정보가 필요하므로, AWS에 있는 connectionInfo.json과 동기화 잘하고,
## 돌린 후에도 최신화 된 connectionInfo.json을 AWS로 업로드해야한다.
from utils import find_con_path, send_to_gbq
from customerprivacy import backfill
import json

con_path = find_con_path()
with open(con_path+"connectionInfo.json", "r") as f:
    data = json.load(f)
auth_key = data["auth_key"]
json_file_path = con_path+"dbwisely-v2-01bfe15ef302.json"

startdate = "2022-12-13"
enddate = "2022-12-14"
cp_frontfill = backfill(startdate, enddate, auth_key, con_path)
print(cp_frontfill)
send_to_gbq(cp_frontfill,"dsCafe24", "tbCustomerPrivacy", json_file_path=json_file_path, if_exists="append")