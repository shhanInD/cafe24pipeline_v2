import requests
import pandas as pd
import time
from utils import get_headers,  send_slack_message
import re
import json
from utils import find_con_path, get_and_refresh_accesstoken, send_to_gbq


pd.set_option('mode.chained_assignment',  None)

def product_update(acstok,
           product_str = "product_no,product_code,custom_product_code,product_name,price,display,selling",
           limit = 100,
           version = "2022-12-01"
           ):

    headers = get_headers(acstok, version)
    products_columns = [i for i in product_str.split(",")]
    products_data = pd.DataFrame(columns=products_columns)
    end = "2030-01-01 00:00:00"
    i = 0

    while i < 51:
        offset = i * 100
        # print("offset : ", offset)
        url = f"https://wiselycompany.cafe24api.com/api/v2/admin/products"
        params = {
            'fields': product_str,
            'created_start_date': '2017-01-01 00:00:00',
            'created_end_date': end,
            'limit': limit,
            'offset': offset
        }

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
                    text = f"**테스트중입니다.**\n상품 데이터 : timeout 발생"
                    send_slack_message(text)
                    break
        # response의 json화
        try:
            response = response.json()
        except requests.exceptions.JSONDecodeError:
            text = f"**테스트중입니다.**\n상품 데이터 : response.json()에서 오류 발생"
            send_slack_message(text)

        try:
            data1 = pd.DataFrame(response["products"])
            time.sleep(0.5)
            try:
                new_data = data1[products_columns]
                products_data = pd.concat([products_data, new_data])
                # print(len(products_data))
                if len(new_data) < 100:  # 100개 이하이면 그 다음차례에 가져올 데이터가 없다는 뜻이므로 이번판 돌고 끝나게 button = 0으로 while문 반복 탈출하도록.
                    # print(date, f": 여기 끝! (offset : {offset})")
                    break
                else:
                    if i == 50:  # offset 5000까지 돌았는데 100개의 상품이 있다면 등록된 상품이 5000개 이상이라는 것.
                        # 따라서 end를 갱신하여 더 찾게하고 offset을 0으로 초기화
                        end = new_data[["created_date"]].min().values[0].replace("T", " ").replace("+09:00", "")
                        # print("여기 오나?")
                        time.sleep(10)
                        i = 0
                    else:
                        i += 1
                        # offset 8000이 아니고 한번에 가져올 리부가 100개일 경우 다음 반복문 타게하기
                        # pass

            except Exception as e1:  # 데이터 없는 경우
                print(e1)
                # print(start, "부터 ", end, ", offset은 ", offset)
                break
        except Exception as e2:  # 액세스토큰 문제 혹은 API response에 문제가 있어 customerprivacy 키값이 없는 경우
            print(e2)
            text = f"**테스트중입니다.**\n상품 품목 데이터 : response에 문제있어 중단됨"
            send_slack_message(text)
            print("response에 문제있어 중단")
            break
    return products_data

def variant_update(acstok, product_no,
           variant_str = "product_no,variants",
           embed = "variants",
           version = "2022-12-01"
           ):
    headers = get_headers(acstok, version)
    url = f"https://wiselycompany.cafe24api.com/api/v2/admin/products/{product_no}"
    params = {
        'fields': "product_no,variants",
        'embed' : embed
    }

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
                text = f"**테스트중입니다.**\n상품 품목 데이터 : timeout 발생"
                send_slack_message(text)
    # response의 json화
    try:
        response = response.json()
    except requests.exceptions.JSONDecodeError:
        text = f"**테스트중입니다.**\n상품 품목 데이터 : response.json()에서 오류 발생"
        send_slack_message(text)
    # DataFrame화
    try:
        data = pd.DataFrame(response["product"])
    except Exception as e2:  # 액세스토큰 문제 혹은 API response에 문제가 있어 customerprivacy 키값이 없는 경우
        print(e2)
        text = f"**테스트중입니다.**\n상품 품목 데이터 : response에 문제있어 중단됨"
        send_slack_message(text)
        print("response에 문제있어 중단")

    return data



con_path = find_con_path()
# searchdate = datetime.strftime(date.today()-timedelta(days=1), "%Y-%m-%d 00:00:00")
# enddate = datetime.strftime(date.today(), "%Y-%m-%d 00:00:00")

# startdate = "2022-12-09 00:00:00"
# enddate = "2023-01-26 00:00:00"
con_file_name = "connectionInfo_products"
with open(con_path+f"{con_file_name}.json", "r") as f:
    data = json.load(f)
auth_key = data["auth_key"]
json_file_path = con_path+"dbwisely-v2-01bfe15ef302.json"

acstok, asctok_expdt, reftok, reftok_expdt = get_and_refresh_accesstoken(auth_key, con_path, con_file_name)
products = product_update(acstok)
# print(products.head(10))
need_variants_products_list = list(products[products["custom_product_code"]==""]["product_no"].values)
variants_data = pd.DataFrame(columns = ["product_no", "variants"])
for product_no in need_variants_products_list:
    variants_new = variant_update(acstok,product_no)
    variants_data = pd.concat([variants_data, variants_new])
    time.sleep(0.5)
variants_data = variants_data.reset_index(drop = True)
products.to_excel("상품테스트.xlsx", index = False)
variants_data.to_excel("품목테스트.xlsx", index = False)

# print(variants_data.head(50))
# joined_data.to_excel("상품품목테스트.xlsx", index = False)
variants_data["variant_regex"] = variants_data["variants"].apply(lambda x: re.search("\'options\'\: (.+?), \'custom_variant_code\'", str(x)).group(1) if pd.notna(x) else "")
variants_data["variant_value"] = variants_data["variant_regex"].apply(lambda x: re.findall("\'value\'\: \'(.+?)\'\}", x) if x is not None else "")
variants_data["varinat_name"] = variants_data["variant_value"].apply(lambda x: "("+str(x).replace("[","").replace("]","").replace("'", "")+")" if x != [] else "")

variants_data["add_price_dict"] = variants_data["variants"].apply(lambda x: re.findall(r"additional_amount\'\: \'[0-9]+\.00\'", str(x)) if x is not None else None)
variants_data["additional_price"] = variants_data["add_price_dict"].apply(lambda x : float(x[0].split(":")[-1].replace("'", "")) if x != [] else 0)

variants_data["variants_dict"] = variants_data["variants"].apply(lambda x: re.findall(r"custom_variant_code\'\: \'[0-9]+\'", str(x)) if x is not None else None)
variants_data["custom_variant_code"] = variants_data["variants_dict"].apply(lambda x : x[0].split(":")[-1].replace("'", "") if x != [] else "")


joined_data = pd.merge(products, variants_data, on = "product_no", how = "left")
joined_data["custom_product_code"] =  joined_data["custom_product_code"].apply(lambda x : str(x) if pd.notna(x) else "")
joined_data["productcode"] = joined_data["custom_product_code"]+joined_data["custom_variant_code"]
joined_data["productcode"] = joined_data["productcode"].apply(lambda x : str(x).strip() if str(x).strip() != "nan" else "")
joined_data["additional_price"] = joined_data["additional_price"].fillna(0)
joined_data["len"] = joined_data["productcode"].apply(lambda x : len(x))
joined_data = joined_data[joined_data["len"]>0][["product_no", "product_code", "productcode", "product_name", "varinat_name", "display", "selling", "price", "additional_price"]]
joined_data["product_no"] = joined_data["product_no"].astype('str')
joined_data["productcode"] = joined_data["productcode"].astype('str')
joined_data["varinat_name"] = joined_data["varinat_name"].astype('str')
joined_data["price"] = joined_data["price"].astype('str')
joined_data["additional_price"] = joined_data["additional_price"].astype('str')
# print(joined_data.head(30))
# joined_data.to_excel("상품품목테스트.xlsx", index = False)
send_to_gbq(joined_data, "dsCafe24","tbProductVariants_Raw", json_file_path=json_file_path, if_exists="replace")


# products.drop_duplicates().to_excel("상품테스트.xlsx", index = False)










