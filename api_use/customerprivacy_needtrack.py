import requests
import pandas as pd
import time
from utils import get_headers,  send_slack_message
pd.set_option('mode.chained_assignment',  None)

def fillup(  backfill_startdate,
             backfill_enddate,
             acstok,
             points_str='member_id,created_date,total_points,available_points,used_points,last_login_date,sms,news_mail',
             limit=1000, version="2022-12-01"):
    headers = get_headers(acstok, version)
    points_columns = [i for i in points_str.split(",")]
    points_data = pd.DataFrame(columns = points_columns)
    button = 1
    while button == 1:
        url = f"https://wiselycompany.cafe24api.com/api/v2/admin/customersprivacy"
        params = {
            'date_type' : 'join',
            'fields' : points_str,
            'start_date' : backfill_startdate,
            'end_date': backfill_enddate,
            'limit' : limit
        }
        try:
            response = requests.get(url, params = params, headers=headers, timeout = 3)
        except requests.exceptions.ConnectionError:
            try :
                # print("2트 중....................!")
                response = requests.get(url, params = params, headers=headers, timeout = 3)
            except requests.exceptions.ConnectionError:
                try:
                    # print("3트 중....................!")
                    response = requests.get(url, params=params, headers=headers, timeout = 3)
                except requests.exceptions.ConnectionError:
                    text = f"{backfill_startdate}~{backfill_enddate}에서 timeout 발생"
                    send_slack_message(text)
                    break

        try :
            response = response.json()
        except requests.exceptions.JSONDecodeError :
            text = f"{backfill_startdate}~{backfill_enddate}에서 response.json()에서 오류 발생"
            send_slack_message(text)
        try:
            data1 = pd.DataFrame(response["customersprivacy"])
            time.sleep(0.5)
            try:
                new_data = data1[points_columns]
                if len(new_data) < 1000: # 1000개 이하이면 그 다음차례에 가져올 데이터가 없다는 뜻이므로 이번판 돌고 끝나게 button = 0으로 while문 반복 탈출하도록.
                    button = 0
                else:
                    pass
                # 데이터 테이블 합치기
                points_data = pd.concat([points_data, new_data])
                # print(new_data.head(1))
                # print(len(points_data))
                # 다음판 돌 때 backfill_enddate를 업데이트해서 search할 영역을 줄여가도록
                backfill_enddate = new_data[["created_date"]].min().values[0].replace("T", " ").replace("+09:00", "")
            except Exception as e1: # 데이터 없는 경우
                # print(e1)
                break
        except Exception as e2: # 액세스토큰 문제 혹은 API response에 문제가 있어 customerprivacy 키값이 없는 경우
            # print(e2)
            text = f"{backfill_startdate}~{backfill_enddate}에서 response에 문제있어 중단됨"
            send_slack_message(text)
            print("response에 문제있어 중단")
            break
    return points_data


