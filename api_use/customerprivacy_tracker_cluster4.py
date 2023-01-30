import json
from utils import find_con_path, get_and_refresh_accesstoken, send_to_gbq
from customerprivacy_needtrack import fillup

# Customer Privacy 가져오기 불러오기

con_path = find_con_path()
startdate = "2022-01-01 00:00:00"
enddate = "2023-01-01 00:00:00"
con_file_name = "connectionInfo_realtime_4"
with open(con_path+f"{con_file_name}.json", "r") as f:
    data = json.load(f)
auth_key = data["auth_key"]
json_file_path = con_path+"dbwisely-v2-01bfe15ef302.json"

acstok, asctok_expdt, reftok, reftok_expdt = get_and_refresh_accesstoken(auth_key, con_path, con_file_name)

need_track = fillup(startdate, enddate, acstok)
need_track = need_track.drop_duplicates()

send_to_gbq(need_track, "dsCafe24","tbCustomerPrivacy_needtrack_cluster4", json_file_path=json_file_path, if_exists="replace")