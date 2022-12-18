import os
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from utils import get_headers, get_and_refresh_accesstoken, send_to_gbq

def backfill(backfill_startdate,
             backfill_enddate,
             acstok,
             orders_str='order_id,order_date,payment_date,subscription,member_id,payment_amount,payment_method_name,payment_method,order_from_mobile,use_escrow,transaction_id,cancel_date,actual_order_amount,items,receivers,buyer',
             limit=1000, version="2022-09-01",
             ):
    orders_columns = [i for i in orders_str.split(",")]
    orders_data = pd.DataFrame(columns=orders_columns)

    date_range = list(
        datetime.strftime(i, "%Y-%m-%d") for i in pd.date_range(backfill_startdate, backfill_enddate, freq="D"))

    # for date in ["2022-12-07","2022-12-08"]:
    for date in date_range:
        mi = 0
        next_date = date
        for hour in [0, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]:
            if hour == 9:
                hour = "09"
                nexthour = "10"
            elif hour == 0:
                hour = "00"
                nexthour = "07"
            elif len(str(hour)) == 1:  # 0,9 가 아니면 (7,8 이면)
                nexthour = "0" + str(hour + 1)
                hour = "0" + str(hour)

            elif hour == 23:
                hour = "23"
                nexthour = "00"
                next_date = datetime.strftime(datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1), "%Y-%m-%d")
            else:
                nexthour = str(hour + 1)
                hour = str(hour)

            url1 = f"https://wiselycompany.cafe24api.com/api/v2/admin/orders?start_date={date} {hour}:00:00&end_date={next_date} {nexthour}:00:00&limit={limit}&embed=items,receivers,buyer,return,cancellation,exchange"
            response = requests.request("GET", url1, headers=get_headers(acstok, version)).json()
            try:
                data1 = pd.DataFrame(response["orders"])
                time.sleep(0.5)
                try:
                    new_data = data1[orders_columns]
                    orders_data = pd.concat([orders_data, new_data])
                    #             print(len(new_data))
                    mi += len(new_data)
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

    # # receivers
    # from_receivers = [
    #     'name',
    #     'cellphone',
    #     'shipping_code'
    #
    # ]

    for aoa in from_actual_order_amount:
        orders_data[aoa] = orders_data['actual_order_amount'].apply(lambda x: x[aoa])  # float 오류로 제거(by.tax)

    # for rcv in from_receivers:
    #     orders_data[rcv] = orders_data['receivers'].apply(lambda x: [i[rcv] for i in x])

    return orders_data


def frontfill(acstok,
              interval_minute=5,
              orders_str='order_id,order_date,payment_date,subscription,member_id,payment_amount,payment_method_name,payment_method,order_from_mobile,use_escrow,transaction_id,cancel_date,actual_order_amount,items,receivers,buyer',
              limit=1000, version="2022-09-01"):
    orders_columns = [i for i in orders_str.split(",")]

    orders_data_ = pd.DataFrame(columns=orders_columns)
    now = datetime.now()
    # 현재 시간에서 5분 빼기
    last_min = now - timedelta(minutes=interval_minute)
    # 5분 단위로 변경 (예: last_min이 12시16분이면 12시 15분으로 변경)
    frontfill_endtime = last_min.replace(minute=last_min.minute - last_min.minute % interval_minute, second=0,
                                         microsecond=0)
    # frontfill_endtime보다 5분 전.
    frontfill_starttime = frontfill_endtime - timedelta(minutes=interval_minute)
    # frontfill_starttime, frontfill_endtime을 문자열로 변ㄱ셩
    frontfill_starttime_str = datetime.strftime(frontfill_starttime, "%Y-%m-%d %H:%M:00")
    frontfill_endtime_str = datetime.strftime(frontfill_endtime, "%Y-%m-%d %H:%M:00")

    mi = 0
    url1 = f"https://wiselycompany.cafe24api.com/api/v2/admin/orders?start_date={frontfill_starttime_str}&end_date={frontfill_endtime_str}&limit={limit}&embed=items,receivers,buyer,return,cancellation,exchange"

    response = requests.request("GET", url1, headers=get_headers(acstok, version)).json()
    try:
        data1 = pd.DataFrame(response["orders"])
        time.sleep(0.5)
        try:
            new_data = data1[orders_columns]
            orders_data = pd.concat([orders_data_, new_data])
            mi += len(new_data)
        except Exception as e1:
            print(e1)
            orders_data = orders_data_
    except Exception as e2:
        print(e2)
        orders_data = orders_data_

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

    # # receivers
    # from_receivers = [
    #     'name',
    #     'cellphone',
    #     'shipping_code'
    # ]

    for aoa in from_actual_order_amount:
       orders_data[aoa] = orders_data['actual_order_amount'].apply(lambda x: x[aoa])  # float 오류로 제거(by.tax)

    # for rcv in from_receivers:
    #    orders_data[rcv] = orders_data['receivers'].apply(lambda x: ",".join(str(s) for s in [i[rcv] for i in x]))

    return orders_data


def save_log(df, path, filename):
    # path에 파일이 하나도 없으면, 엑셀로 filename으로 저장하기
    if len(os.listdir(path)) == 0:
        df.to_excel(path + filename + ".xlsx", index=False)
    # path 폴더에 한 파일이라도 있으면
    else:
        # path폴더 안에 저장할 이름의 파일이 있다면, 해당 파일에 Concat
        if filename + ".xlsx" in os.listdir(path):
            prior = pd.read_excel(path + filename + ".xlsx")
            after = pd.concat([prior, df])
            after.to_excel(path + filename + ".xlsx", index=False)
        # path폴더 안에 저장할 이름의 파일이 없다면, 엑셀로 filename으로 저장하기 새로 저장하기
        else:
            df.to_excel(path + filename + ".xlsx", index=False)

# load check

# import json
# from datetime import datetime, date
# from utils import find_con_path, send_to_gbq, get_and_refresh_accesstoken
#
# when = datetime.strftime(date.today(), "%Y-%m-%d")
# nowtime = datetime.now()
#
# # Customer Privacy 가져오기 불러오기
# con_path = find_con_path()
# with open(con_path+"connectionInfo.json", "r") as f:
#     data = json.load(f)
# auth_key = data["auth_key"]
# json_file_path = con_path+"dbwisely-v2-01bfe15ef302.json"
#
# acstok, asctok_expdt, reftok, reftok_expdt = get_and_refresh_accesstoken(auth_key, con_path)
#
# # orders_backfill = backfill('20221217', '20221217',auth_key='WkJTclhlUzJLYUxRYjl5TFpydXRrRzpsbW1RREJRUU54Y3FDTkVBQjFEek1D', con_path='/Users/wisely/Dropbox/Future/Data analyst/Company/wisely/Cafe24API/')
# orders_frontfill = frontfill(acstok)
# orders_frontfill_distinct_rows = orders_frontfill[[
#     "order_id", "member_id", "transaction_id", "order_date", "payment_date",  "order_from_mobile",
#     "use_escrow", "actual_order_amount",
#     "payment_amount", "cancel_date",
# ]]
# orders_frontfill_distinct_rows["actual_order_amount"] = orders_frontfill_distinct_rows["actual_order_amount"].astype('str')
#
# orders_frontfill_items = orders_frontfill[["order_id", "items"]]
# import numpy as np
# item_explode = np.dstack(
#     (np.repeat(
#         orders_frontfill_items["order_id"].values, list(map(len, orders_frontfill_items["items"].values))
#         ),
#     np.concatenate(orders_frontfill_items["items"].values)))
# orders_frontfill_items_explode = pd.DataFrame(data = item_explode[0], columns = orders_frontfill_items.columns)
# def delete_unneeded(item):
#     item.pop("additional_option_values")
#     item.pop("original_item_no")
#     item.pop("options")
#     return item
# orders_frontfill_items_explode["items"] = orders_frontfill_items_explode["items"].apply(delete_unneeded)
# orders_frontfill_items_explode["items"] = orders_frontfill_items_explode["items"].astype('str')
# orders_frontfill_items_explode["items"] = orders_frontfill_items_explode["items"].apply(lambda x : x.replace("None", "''"))
#
#
# send_to_gbq(orders_frontfill_distinct_rows, "dsCafe24","tbOrder", json_file_path=json_file_path, if_exists="append")
# send_to_gbq(orders_frontfill_items_explode, "dsCafe24","tbOrderItems", json_file_path=json_file_path, if_exists="append")

