import json
import datetime
from utils import find_con_path, send_to_gbq, get_and_refresh_accesstoken
from customerprivacy import frontfill as cp_ff
from customerprivacy import save_log as cp_sl
print("---------------------------------------")
when = datetime.datetime.strftime(datetime.date.today(), "%Y-%m-%d")
whentime = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")
# print(whentime, "돌이갑니다.")

# Customer Privacy 가져오기 불러오기
con_path = find_con_path()
with open(con_path+"connectionInfo.json", "r") as f:
    data = json.load(f)
auth_key = data["auth_key"]
json_file_path = con_path+"dbwisely-v2-01bfe15ef302.json"

acstok, asctok_expdt, reftok, reftok_expdt = get_and_refresh_accesstoken(auth_key, con_path)

cp_frontfill = cp_ff(acstok)
log_path = con_path.replace("/connection/", "/log/dbMembers/customerprivacy/")
cp_sl(cp_frontfill, log_path, when)
send_to_gbq(cp_frontfill,"dsCafe24", "tbCustomerPrivacy", json_file_path=json_file_path, if_exists="append")
whentime_2 = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")
data_qnt = len(cp_frontfill)
print(whentime_2, "에 ", data_qnt, "명의 데이터가 추가됩니다.")
