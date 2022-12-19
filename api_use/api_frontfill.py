import json
import datetime
import numpy as np
import pandas as pd
from utils import find_con_path, send_to_gbq, get_and_refresh_accesstoken
from customerprivacy import frontfill as cp_ff
from customerprivacy import save_log as cp_sl
from orders import frontfill as o_ff
from orders import save_log as o_sl

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
print(whentime_2, "에 회원", data_qnt, "명의 데이터가 추가됩니다.")

# Order, OrderItems 가져오기 불러오기
o_frontfill = o_ff(acstok)
if len(o_frontfill) > 0 :
    try :
        orders_frontfill_distinct_rows = o_frontfill[[
            "order_id", "member_id", "transaction_id", "order_date", "payment_date",  "order_from_mobile",
            "use_escrow", "actual_order_amount",
            "payment_amount", "cancel_date", "subscription"
        ]]
        orders_frontfill_distinct_rows["actual_order_amount"] = orders_frontfill_distinct_rows["actual_order_amount"].astype('str')
        orders_frontfill_items = o_frontfill[["order_id", "items"]]
        item_explode = np.dstack(
            (np.repeat(
                orders_frontfill_items["order_id"].values, list(map(len, orders_frontfill_items["items"].values))
                ),
            np.concatenate(orders_frontfill_items["items"].values)))
        orders_frontfill_items_explode = pd.DataFrame(data = item_explode[0], columns = orders_frontfill_items.columns)
        def delete_unneeded(item):
            item.pop("additional_option_values")
            item.pop("original_item_no")
            item.pop("options")
            return item
        orders_frontfill_items_explode["items"] = orders_frontfill_items_explode["items"].apply(delete_unneeded)
        orders_frontfill_items_explode["items"] = orders_frontfill_items_explode["items"].astype('str')
        orders_frontfill_items_explode["items"] = orders_frontfill_items_explode["items"].apply(lambda x : x.replace("None", "''"))
        log_path_orders = con_path.replace("/connection/", "/log/dbOrders/orders/")
        log_path_orderitems = con_path.replace("/connection/", "/log/dbOrders/orderitems/")

        o_sl(orders_frontfill_distinct_rows, log_path, when)
        o_sl(orders_frontfill_items_explode, log_path, when)
        send_to_gbq(orders_frontfill_distinct_rows, "dsCafe24","tbOrder", json_file_path=json_file_path, if_exists="append")
        send_to_gbq(orders_frontfill_items_explode, "dsCafe24","tbOrderItems", json_file_path=json_file_path, if_exists="append")

        data_qnt_orders = len(orders_frontfill_distinct_rows)
        data_qnt_orderitems = len(orders_frontfill_items_explode)
        whentime_2 = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")
        print(whentime_2, "에 주문", data_qnt_orders, "건의 데이터와 주문 품목", data_qnt_orderitems," 건의 데이터가 추가됩니다.")
    except Exception as e:
        print(e)
        pass
else :
    whentime_2 = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")
    print(whentime_2, "에 0건의 주문이 기록됐습니다.")
    pass
