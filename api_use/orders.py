#!/usr/bin/env python
# coding: utf-8
import os
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from utils import get_headers, get_and_refresh_accesstoken, send_to_gbq
# from cafe24_test.utils import send_to_gbq, get_headers, get_and_refresh_accesstoken




def backfill(backfill_startdate,
             backfill_enddate,
             auth_key, con_path,
             orders_str='order_id,order_date,payment_date,subscription,member_id,payment_amount,payment_method_name,payment_method,order_from_mobile,use_escrow,transaction_id,cancel_date,actual_order_amount,items,receivers,buyer',
             limit=1000, version="2022-09-01",
            ):
    orders_columns = [i for i in orders_str.split(",")]

    acstok, asctok_expdt, reftok, reftok_expdt = get_and_refresh_accesstoken(auth_key, con_path)
    orders_data = pd.DataFrame(columns=orders_columns)

    date_range = list(datetime.strftime(i, "%Y-%m-%d") for i in pd.date_range(backfill_startdate, backfill_enddate, freq="D"))

    # for date in ["2022-12-07","2022-12-08"]:
    for date in date_range:
        mi = 0
        if date == '2022-11-24':
            timeslots = [
                    "start_date=2022-11-24 00:00:00&end_date=2022-11-24 10:00:00",
                    "start_date=2022-11-24 10:00:00&end_date=2022-11-24 10:20:00",
                    "start_date=2022-11-24 10:20:00&end_date=2022-11-24 10:40:00",
                    "start_date=2022-11-24 10:40:00&end_date=2022-11-24 11:00:00",
                    "start_date=2022-11-24 11:00:00&end_date=2022-11-24 11:20:00",
                    "start_date=2022-11-24 11:20:00&end_date=2022-11-24 11:40:00",
                    "start_date=2022-11-24 11:40:00&end_date=2022-11-24 12:00:00",
                    "start_date=2022-11-24 12:00:00&end_date=2022-11-24 12:20:00",
                    "start_date=2022-11-24 12:20:00&end_date=2022-11-24 12:40:00",
                    "start_date=2022-11-24 12:40:00&end_date=2022-11-24 12:50:00",
                    "start_date=2022-11-24 12:50:00&end_date=2022-11-24 13:00:00",
                    "start_date=2022-11-24 13:00:00&end_date=2022-11-24 13:30:00",
                    "start_date=2022-11-24 13:30:00&end_date=2022-11-24 14:00:00",
                    "start_date=2022-11-24 14:00:00&end_date=2022-11-24 16:00:00",
                    "start_date=2022-11-24 16:00:00&end_date=2022-11-25 00:00:00"   
                ]
            for timeslot in timeslots:
                mi = 0
                
                # url1 = f"https://wiselycompany.cafe24api.com/api/v2/admin/customersprivacy?date_type=join&{timeslot}&fields={privacy_str}&limit=1000"
                url1 = f"https://wiselycompany.cafe24api.com/api/v2/admin/orders?{timeslot}&limit={limit}&embed=items,receivers,buyer"
            
            #         print(url1)
                data1 = pd.DataFrame(requests.request("GET", url1, headers=get_headers(acstok, version)).json()["orders"])
                time.sleep(0.5)
            #         print(f"{hour}시와 {nexthour}+1사이 회원가입자 수 : {len(data1)}")
                try:
                    new_data = data1[orders_columns]
                    orders_data = pd.concat([orders_data, new_data])
            #             print(len(new_data))
                    mi +=len(new_data)
                except Exception as e:
                    print(e)
                    continue
#                 print(timeslot, "에 회원가입한 수 : ",mi)
        else:
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

                # url1 = f"https://wiselycompany.cafe24api.com/api/v2/admin/customersprivacy?date_type=join&start_date={date} {hour}:00:00&end_date={next_date} {nexthour}:00:00&fields={privacy_str}&limit={limit}"
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

#             print(date, "에 회원가입한 수 : ", mi)



    # actual_order_amount
    from_actual_order_amount=[
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
                        "tax"
                        ]
    
    
    # items
    from_items=[
                        "item_no",
                        "order_item_code",
                        "variant_code",
                        "product_no",
                        "product_code",
                        "custom_product_code",
                        "custom_variant_code",
                        "option_id",
                        "option_value",
                        "additional_option_value",
                        "product_name",
                        "product_price",
                        "option_price",
                        "additional_discount_price",
                        "coupon_discount_price",
                        "app_item_discount_amount",
                        "payment_amount",
                        "quantity",
                        "product_tax_type",
                        "order_status",
                        "order_status_additional_info",
                        "subscription",
                        ]
    
    
    # receivers
    from_receivers=[
                        'name',
                        'cellphone',
                        'shipping_code'
                      
                        ]
    
    
    for aoa in from_actual_order_amount:
        orders_data[aoa] = orders_data['actual_order_amount'].apply(lambda x: x[aoa])  # float 오류로 제거(by.tax)
    
    
    for its in from_items:
        orders_data[its]=orders_data['items'].apply(lambda x: [ i[its] for i in x ])
    
    
    for rcv in from_receivers:
        orders_data[rcv]=orders_data['receivers'].apply(lambda x: [ i[rcv] for i in x ])

    
    
    return orders_data

    
    




def frontfill(auth_key, con_path, 
              interval_minute = 5,
              orders_str='order_id,order_date,payment_date,subscription,member_id,payment_amount,payment_method_name,payment_method,order_from_mobile,use_escrow,transaction_id,cancel_date,actual_order_amount,items,receivers,buyer',
              limit=1000, version="2022-09-01"):
    orders_columns = [i for i in orders_str.split(",")]

    acstok, asctok_expdt, reftok, reftok_expdt = get_and_refresh_accesstoken(auth_key, con_path)
    
    orders_data_ = pd.DataFrame(columns=orders_columns)
    now = datetime.now()
    # 현재 시간에서 5분 빼기
    last_min = now-timedelta(minutes= 5)
    # 5분 단위로 변경 (예: last_min이 12시16분이면 12시 15분으로 변경)
    frontfill_endtime = last_min.replace(minute=last_min.minute-last_min.minute%interval_minute, second = 0, microsecond = 0)
    # frontfill_endtime보다 5분 전. 
    frontfill_starttime = frontfill_endtime-timedelta(minutes=5)
    # frontfill_starttime, frontfill_endtime을 문자열로 변ㄱ셩
    frontfill_starttime_str = datetime.strftime(frontfill_starttime, "%Y-%m-%d %H:%M:00")
    frontfill_endtime_str = datetime.strftime(frontfill_endtime, "%Y-%m-%d %H:%M:00")
    
    mi = 0
    # url1 = f"https://wiselycompany.cafe24api.com/api/v2/admin/customersprivacy?date_type=join&start_date={frontfill_starttime_str}&end_date={frontfill_endtime_str}&fields={privacy_str}&limit={limit}"
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
    from_actual_order_amount=[
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
                        "tax"
                        ]
    
    
    # items
    from_items=[
                        "item_no",
                        "order_item_code",
                        "variant_code",
                        "product_no",
                        "product_code",
                        "custom_product_code",
                        "custom_variant_code",
                        "option_id",
                        "option_value",
                        "additional_option_value",
                        "product_name",
                        "product_price",
                        "option_price",
                        "additional_discount_price",
                        "coupon_discount_price",
                        "app_item_discount_amount",
                        "payment_amount",
                        "quantity",
                        "product_tax_type",
                        "order_status",
                        "order_status_additional_info",
                        "subscription",
                        ]
    
    
    # receivers
    from_receivers=[
                        'name',
                        'cellphone',
                        'shipping_code'
                      
                        ]
    
    
    for aoa in from_actual_order_amount:
        orders_data[aoa] = orders_data['actual_order_amount'].apply(lambda x: x[aoa])  # float 오류로 제거(by.tax)
    
    
    for its in from_items:
        orders_data[its]=orders_data['items'].apply(lambda x: [ i[its] for i in x ])
    
    
    for rcv in from_receivers:
        orders_data[rcv]=orders_data['receivers'].apply(lambda x: [ i[rcv] for i in x ])


    return orders_data
    # print(len(customerprivacy_data))






def save_log(df, path, filename):
    # path에 파일이 하나도 없으면, 엑셀로 filename으로 저장하기
    if len(os.listdir(path)) == 0:
        df.to_excel(path+filename+".xlsx", index=False)
    # path 폴더에 한 파일이라도 있으면
    else :
        # path폴더 안에 저장할 이름의 파일이 있다면, 해당 파일에 Concat
        if filename+".xlsx" in os.listdir(path):
            prior = pd.read_excel(path+filename+".xlsx")
            after = pd.concat([prior, df])
            after.to_excel(path+filename+".xlsx", index=False)
        # path폴더 안에 저장할 이름의 파일이 없다면, 엑셀로 filename으로 저장하기 새로 저장하기
        else :
            df.to_excel(path + filename + ".xlsx", index=False)





# load check
# orders_backfill = backfill('20221217', '20221217',auth_key='WkJTclhlUzJLYUxRYjl5TFpydXRrRzpsbW1RREJRUU54Y3FDTkVBQjFEek1D', con_path='/Users/wisely/Dropbox/Future/Data analyst/Company/wisely/Cafe24API/')
# orders_frontfill = frontfill(auth_key='WkJTclhlUzJLYUxRYjl5TFpydXRrRzpsbW1RREJRUU54Y3FDTkVBQjFEek1D', con_path='/Users/wisely/Dropbox/Future/Data analyst/Company/wisely/Cafe24API/')

# orders_backfill.iloc[0]
