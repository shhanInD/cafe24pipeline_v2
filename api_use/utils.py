import requests
import os
import json
import pandas_gbq
from google.oauth2 import service_account

def find_con_path():
    if os.getcwd() == "/home/ubuntu": # AWS에 올렸을 때용
        pathstr = "/home/ubuntu/automation/Cafe24Pipeline/cafe24pipeline/pathlists.json" # pathlists.json이 있는 곳
    else :
        pathstr = '../pathlists.json'
    with open(pathstr, "r") as jsonfile:
        pathInfo = json.load(jsonfile)
    for subject in pathInfo["connection"]["path_list"]:
        path = pathInfo["connection"]["path_list"][subject]
        try:
            os.listdir(path)
            return path
        except: continue

def getpath():
    print(os.getcwd())

def send_to_gbq(df, dataset, tb_name,
                json_file_path, #= 'dbwisely-v2-01bfe15ef302.json', #<- 싸쿤용,
                # scopes = [
                #             'https://www.googleapis.com/auth/cloud-platform',
                #             'https://www.googleapis.com/auth/drive',
                #             'https://www.googleapis.com/auth/bigquery'
                #         ],
                project = "dbwisely-v2",
                if_exists = 'replace'
                ):
    # json_file_name = '경로쓰세요/dbwisely-v2-01bfe15ef302.json'
    json_file_name = json_file_path

    # SCOPES = scopes
    credentials = service_account.Credentials.from_service_account_file(json_file_name)
    project_id = project
    #새로 테이블 만들것이라면 굳이 빅쿼리에서 미리 스킴 짤필요 없이 여기서 테이블 이름 지정하면 됩니다.
    destination_table = f'{dataset}.{tb_name}'

    # print("데이터 빅쿼리로 옮길 준비")
    pandas_gbq.to_gbq(df, destination_table,project_id,if_exists=if_exists,credentials=credentials)
    # print("빅쿼리로 이관 완료")

def get_and_refresh_accesstoken(authorization_key, connection_file_path, file_name):
    # 경로내 액세스/리프레시토큰 파일 있는지 확인하기
    thereis_json = False
    files = [f for f in os.listdir(connection_file_path)]
    con_json_file_path = connection_file_path+f"{file_name}.json"
    if f"{file_name}.json" in files:
        with open(con_json_file_path, "r") as jsonfile:
            data = json.load(jsonfile)
        refresh_token = data["refresh_token"]
        isthere_json = True

    # 리프레시토큰으로 새로운 액세스 토큰 받기
    refresh_token_url = "https://wiselycompany.cafe24api.com/api/v2/oauth/token"
    headers = {
        'Authorization' : f'Basic {authorization_key}',
        'Content-Type' : 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type':'refresh_token',
        'refresh_token':f'{refresh_token}'
    }

    refresh_rq = requests.request("POST", refresh_token_url, headers = headers, data = data)

    token_json = refresh_rq.json()

    new_refresh_token = token_json["refresh_token"]
    new_refresh_token_exp_date = token_json["refresh_token_expires_at"]
    new_access_token = token_json["access_token"]
    new_access_token_exp_date = token_json["expires_at"]
    access_scopes = token_json["scopes"]

    print("ㄱㅅtkn : ", new_refresh_token, " , ", new_refresh_token_exp_date, "까지")

    # 토큰 데이터 업데이트하기
    if thereis_json :
        # json 파일 업데이트하기
        with open(con_json_file_path, "r") as jsonfile:
            data = json.load(jsonfile)
        data["auth_key"] = authorization_key
        data["access_token"] = new_access_token
        data["expires_at"] = new_access_token_exp_date
        data["refresh_token"] = new_refresh_token
        data["refresh_token_expires_at"] = new_refresh_token_exp_date
        data["scopes"] = access_scopes

        with open(con_json_file_path, "w", encoding= "utf-8") as jsonfile:
            json.dump(data, jsonfile, ensure_ascii=False, indent="\t")
    else :
        # 처음 json 파일 만들기
        con_data = {}
        con_data["auth_key"] = authorization_key
        con_data["access_token"] = new_access_token
        con_data["expires_at"] = new_access_token_exp_date
        con_data["refresh_token"] = new_refresh_token
        con_data["refresh_token_expires_at"] = new_refresh_token_exp_date
        con_data["scopes"] = access_scopes

        with open(con_json_file_path, "w", encoding = "utf-8") as make_file:
            json.dump(con_data, make_file, ensure_ascii=False, indent="\t")
    
    return new_access_token, new_access_token_exp_date, new_refresh_token, new_refresh_token_exp_date



def get_headers(new_access_token, vers):
    head = {
        'Authorization': 'Bearer ' + str(new_access_token),
        # "Bearer RPJ5JdFlIt5kkn0VUiwm3C", # new_access_token 넣어주기!! (돌릴때마다 변경됨)
        'Content-Type': "application/json",
        'X-Cafe24-Api-Version': vers
    }
    return head

def send_slack_message(text):
    url = 'https://slack webhook url'
    payload = {
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": ":bell:코드 오류:bell:"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": text}},
        ]
    }
    requests.post(url, json=payload)


