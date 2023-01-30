import requests
import pandas as pd
import time
from utils import get_headers,  send_slack_message
from datetime import datetime, timedelta, date
import json
from utils import find_con_path, get_and_refresh_accesstoken, send_to_gbq


pd.set_option('mode.chained_assignment',  None)


def fillup(  backfill_startdate,
             backfill_enddate,
             acstok,
             reviews_str='board_no,article_no,member_id,order_id,product_no,content,rating,input_channel,created_date,naverpay_review_id,attach_file_urls',
             limit=100, version="2022-12-01"):
    headers = get_headers(acstok, version)
    reviews_columns = [i for i in reviews_str.split(",")]
    reviews_data = pd.DataFrame(columns=reviews_columns)

    start_date = backfill_startdate.replace("-","").replace(" 00:00:00", "")
    end_date = backfill_enddate.replace("-","").replace(" 00:00:00", "")
    date_list = pd.date_range(start=start_date, end=end_date, freq='D')
    # print(date_list)
    loopnum = 1
    for date in date_list:
        # print(date)
        start = date
        end = datetime.strftime(datetime.strptime(str(date). replace(" 00:00:00", ""), "%Y-%m-%d")+timedelta(days=1), "%Y-%m-%d 00:00:00")
        print(start, "부터 ", end, "까지 데이터를 긁어옵니다.")
        i = 0
        while i < 81:
            offset = i*100
            # print("offset : ", offset)
            url = f"https://wiselycompany.cafe24api.com/api/v2/admin/boards/4/articles"
            params = {
                'fields': reviews_str,
                'start_date': start,
                'end_date': end,
                'limit': limit,
                'offset' : offset
            }
            # if loopnum > 1:
                # print(start, "부터 ", end, "까지 offset", offset, "에서부터 데이터를 긁어옵니다.")
            # else:
            #     pass

            # response 가져오기 3트
            try:
                response = requests.get(url, params=params, headers=headers, timeout=5)
            except requests.exceptions.ConnectionError:
                try:
                    # print("2트 중....................!")
                    response = requests.get(url, params=params, headers=headers, timeout=5)
                except requests.exceptions.ConnectionError:
                    try:
                        # print("3트 중....................!")
                        response = requests.get(url, params=params, headers=headers, timeout=5)
                    except requests.exceptions.ConnectionError:
                        text = f"**테스트중입니다.**\n리뷰 데이터 : {start}~{end}에서 timeout 발생"
                        send_slack_message(text)
                        break
            #response의 json화
            try:
                response = response.json()
            except requests.exceptions.JSONDecodeError:
                text = f"**테스트중입니다.**\n리뷰 데이터 : {start}~{end}에서 response.json()에서 오류 발생"
                send_slack_message(text)
            #데이터 프레임에 concat하여 저장하기
            try:
                data1 = pd.DataFrame(response["articles"])
                time.sleep(0.5)
                try:
                    new_data = data1[reviews_columns]
                    reviews_data = pd.concat([reviews_data, new_data])
                    # print(len(reviews_data))
                    if len(new_data) < 100: # 100개 이하이면 그 다음차례에 가져올 데이터가 없다는 뜻이므로 이번판 돌고 끝나게 button = 0으로 while문 반복 탈출하도록.
                        # print(date, f": 여기 끝! (offset : {offset})")
                        break
                    else:
                        if i == 80:# offset 8000까지 돌았는데 100개의 리뷰가 있다면 하루에 쌓인 8000개 이상이라는 것
                            # 따라서 end를 갱신하여 더 찾게하고 offset을 0으로 초기화
                            end = new_data[["created_date"]].min().values[0].replace("T", " ").replace("+09:00", "")
                            # print("여기 오나?")
                            time.sleep(10)
                            i = 0
                            loopnum += 1
                        else :
                            i += 1
                            # pass# offset 8000이 아니고 한번에 가져올 리부가 100개일 경우 다음 반복문 타게하기
                except Exception as e1: # 데이터 없는 경우
                    print(e1)
                    # print(start, "부터 ", end, ", offset은 ", offset)
                    break
            except Exception as e2: # 액세스토큰 문제 혹은 API response에 문제가 있어 customerprivacy 키값이 없는 경우
                print(e2)
                text = f"**테스트중입니다.**\n리뷰 데이터 : {start}~{end}에서 response에 문제있어 중단됨"
                send_slack_message(text)
                print("response에 문제있어 중단")
                break
    return reviews_data


con_path = find_con_path()
searchdate = datetime.strftime(date.today()-timedelta(days=1), "%Y-%m-%d 00:00:00")
# enddate = datetime.strftime(date.today(), "%Y-%m-%d 00:00:00")

# startdate = "2022-12-09 00:00:00"
# enddate = "2023-01-26 00:00:00"
con_file_name = "connectionInfo_reviews"
with open(con_path+f"{con_file_name}.json", "r") as f:
    data = json.load(f)
auth_key = data["auth_key"]
json_file_path = con_path+"dbwisely-v2-01bfe15ef302.json"

acstok, asctok_expdt, reftok, reftok_expdt = get_and_refresh_accesstoken(auth_key, con_path, con_file_name)

review = fillup(searchdate, searchdate, acstok)
review["is_photoreview"] = review["attach_file_urls"].apply(lambda x : 0 if len(x)==0 else 1)
# review.drop(["attach_file_urls", "board_no"], axis=1).drop_duplicates().to_excel("리뷰테스트.xlsx", index = False)
# review = review.drop_duplicates()
review = review.drop(["attach_file_urls", "board_no"], axis=1).drop_duplicates()
review["article_no"] = review["article_no"].astype('str')
review["product_no"] = review["product_no"].astype('str')
review["rating"] = review["rating"].astype('str')
review["is_photoreview"] = review["is_photoreview"].astype('str')
send_to_gbq(review, "dsCafe24","tbReviews_Cafe24_Raw", json_file_path=json_file_path, if_exists="append")