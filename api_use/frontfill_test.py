import json
import datetime
from utils import find_con_path, send_to_gbq, get_and_refresh_accesstoken
from customerprivacy import frontfill as cp_ff
from customerprivacy import save_log as cp_sl


when = datetime.datetime.strftime(datetime.date.today(), "%Y-%m-%d")

# Customer Privacy 가져오기 불러오기
con_path = find_con_path()
with open(con_path+"connectionInfo.json", "r") as f:
    data = json.load(f)
auth_key = data["auth_key"]
json_file_path = con_path+"dbwisely-v2-01bfe15ef302.json"

acstok, asctok_expdt, reftok, reftok_expdt = get_and_refresh_accesstoken(auth_key, con_path)

cp_frontfill = cp_ff(acstok)
print(cp_frontfill)
# log_path = con_path.replace("/connection/", "/log/dbMembers/customerprivacy/")
# cp_sl(cp_frontfill, log_path, when)
# send_to_gbq(cp_frontfill,"HSH", "tbCustomerPrivacy_Frontfill", json_file_path=json_file_path, if_exists="append")
# print(log_path)
