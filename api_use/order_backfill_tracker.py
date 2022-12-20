import json
from utils import find_con_path
import requests
import os
import math
import pandas as pd
import time
from datetime import datetime, timedelta, date
from utils import get_headers, get_and_refresh_accesstoken, send_to_gbq
pd.set_option('mode.chained_assignment',  None)


def backfill(backfill_startdate,
             backfill_enddate,
             acstok,
             orders_str='order_id,order_date,payment_date,subscription,member_id,payment_amount,payment_method_name,payment_method,order_from_mobile,use_escrow,transaction_id,cancel_date,actual_order_amount,items,receivers,buyer,cancellation,return,exchange,paid,canceled,shipping_status',
             limit=1000, version="2022-09-01",
             ):
    orders_columns = [i for i in orders_str.split(",")]
    orders_data = pd.DataFrame(columns=orders_columns)

    # print(backfill_startdate)
    # print(backfill_enddate)
    ordercnt_url = f"https://wiselycompany.cafe24api.com/api/v2/admin/orders/count?shop_no=1&start_date={backfill_startdate}&end_date={backfill_enddate}"
    ordercnt = requests.request("GET", ordercnt_url, headers=get_headers(acstok, version)).json()["count"]
    # print(ordercnt)
    # mi = 0
    for i in range(math.ceil(ordercnt/16000)): #offset이 기본 15000까지.
        if i < math.ceil(ordercnt/16000):
            r = 16
        else :
            r = math.ceil(ordercnt/1000)
        for j in range(r):
            # offset = j*1000
            # print(i*16+j)
            url_orders = f"https://wiselycompany.cafe24api.com/api/v2/admin/orders?daet_type=order_date&start_date={backfill_startdate}&end_date={backfill_enddate}&limit={limit}&embed=items,receivers,buyer,return,cancellation,exchange"
            # print(url_orders)
            response = requests.request("GET", url_orders, headers=get_headers(acstok, version)).json()
            try:
                data1 = pd.DataFrame(response["orders"])
                time.sleep(0.5)
                try:
                    new_data = data1[orders_columns]
                    day, timez = new_data["order_date"].min().split("T")
                    timez = timez.replace("+09:00", "")
                    backfill_enddate = " ".join([day, timez])
                    # print(backfill_enddate)
                    # print(new_data.head(3))
                    orders_data = pd.concat([orders_data, new_data])
                except Exception as e1:
                    print(e1)
                    continue
            except Exception as e2:
                print(e2)
                continue

    # actual_order_amount
    from_actual_order_amount = [
        "order_price_amount",
        "shipping_fee",
        "points_spent_amount",
        "credits_spent_amount",
        "coupon_discount_price",
        "coupon_shipping_fee_amount",
        "membership_discount_amount",
        "shipping_fee_discount_amount",
        "set_product_discount_amount",
        "app_discount_amount",
        "point_incentive_amount",
        "total_amount_due",
        "payment_amount",
        "market_other_discount_amount",
        # "tax"
    ]
    #
    # # # receivers
    # # from_receivers = [
    # #     'name',
    # #     'cellphone',
    # #     'shipping_code'
    # #
    # # ]
    #
    for aoa in from_actual_order_amount:
        orders_data[aoa] = orders_data['actual_order_amount'].apply(lambda x: x[aoa])  # float 오류로 제거(by.tax)

    # for rcv in from_receivers:
    #     orders_data[rcv] = orders_data['receivers'].apply(lambda x: [i[rcv] for i in x])
    # print(ordercnt)
    # print(len(orders_data))
    return orders_data


def save_log(df, path, filename):
    df.to_excel(path + filename + ".xlsx", index=False)


whendate = date.today()
when = datetime.strftime(whendate, "%Y-%m-%d")
then = datetime.strftime(whendate - timedelta(days = 6), "%Y-%m-%d")
print(then)
print(when)

nowtime = datetime.now()

# Customer Privacy 가져오기 불러오기
con_path = find_con_path()
with open(con_path+"conenction_fororderbackfill.json", "r") as f:
    data = json.load(f)
auth_key = data["auth_key"]
json_file_path = con_path+"dbwisely-v2-01bfe15ef302.json"

acstok, asctok_expdt, reftok, reftok_expdt = get_and_refresh_accesstoken(auth_key, con_path)

orders_backfill = backfill(then, when, acstok)
orders_backfill_distinct_rows = orders_backfill[[
    "order_id", "member_id", "transaction_id", "order_date", "payment_date", "order_from_mobile",
    "use_escrow", "actual_order_amount",
    "payment_amount", "cancel_date", 'canceled', 'paid', 'shipping_status', 'return', 'exchange'
    #'cancellation', 'return', 'exchange'
]]
orders_backfill_distinct_rows["actual_order_amount"] = orders_backfill_distinct_rows["actual_order_amount"].astype('str', errors='ignore')
orders_backfill_distinct_rows["return_date"] = orders_backfill_distinct_rows["return"].apply(lambda x : x[0]["items"][0]["cancel_date"] if len(x) > 0 else "")
orders_backfill_distinct_rows["exchange_date"] = orders_backfill_distinct_rows["exchange"].apply(lambda x : x[0]["items"][0]["cancel_date"] if len(x) > 0 else "")
orders_backfill_distinct_rows = orders_backfill_distinct_rows.drop(["return", "exchange"], axis = 1)


orders_backfill_items = orders_backfill[["order_id", "items"]]
import numpy as np
item_explode = np.dstack(
    (np.repeat(
        orders_backfill_items["order_id"].values, list(map(len, orders_backfill_items["items"].values))
        ),
    np.concatenate(orders_backfill_items["items"].values)))
orders_backfill_items_explode = pd.DataFrame(data = item_explode[0], columns = orders_backfill_items.columns)
def delete_unneeded(item):
    item.pop("additional_option_values")
    item.pop("original_item_no")
    item.pop("options")
    return item
orders_backfill_items_explode["items"] = orders_backfill_items_explode["items"].apply(delete_unneeded)
orders_backfill_items_explode["items"] = orders_backfill_items_explode["items"].astype('str', errors='ignore')
orders_backfill_items_explode["items"] = orders_backfill_items_explode["items"].apply(lambda x : x.replace("None", "''"))

orders_backfill_distinct_rows = orders_backfill_distinct_rows.drop_duplicates()
orders_backfill_items_explode = orders_backfill_items_explode.drop_duplicates()


send_to_gbq(orders_backfill_distinct_rows, "dsCafe24","tbOrder_track7days", json_file_path=json_file_path, if_exists="replace")
send_to_gbq(orders_backfill_items_explode, "dsCafe24","tbOrderItems_track7days", json_file_path=json_file_path, if_exists="replace")