# coding: utf-8
import os
import re
import json

from celery import Celery, group, chain
import requests
import models

print(os.getenv('RBMQ_HOST'))
app = Celery('main', broker='pyamqp://guest@%s//'%(os.getenv('RBMQ_HOST')))
app.conf.task_routes = {
        'main.to_postgres': {'queue': 'to_postgres'}
        }
HOST = 'https://www.honestbee.tw'

def get_stores():
    resp = requests.get(HOST + '/zh-tw/groceries/stores')
    window_data = json.loads(re.findall('window.__data=(.+?); window.__i18n', resp.text, re.DOTALL)[0])
    brands = window_data['groceries']['brands']['byId']
    print("got stores")
    return brands

@app.task
def get_products(store_id, page=1):
    url = "https://www.honestbee.tw/api/api/stores/{}?sort=price:desc&page={}".format(store_id, page) #&fields[]=departments"
    headers = json.loads(r'''{
        "Accept": "application/vnd.honestbee+json;version=2",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-TW",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Cookie": "connect.sid=s%3AMhmQ1nQHYL_69wnrjuJBgExp5Zv7zVqs.g6m01NNVv%2BOMh1K6KiTVPUOthSZQHjUxe%2F1Qvdt1hgU; ajs_user_id=null; ajs_group_id=null; ajs_anonymous_id=%22d9d3c3ec-2472-4fcf-9abb-df5fc9984c4b%22; _ga=GA1.2.1181326923.1525430384; appier_utmz=%7B%22csr%22%3A%22google%22%2C%22timestamp%22%3A1525430384%7D; _atrk_siteuid=Iv-rYwXkRnTLdhXM; ab.storage.deviceId.b0a7b43e-b927-41a2-8f56-8183f42abb09=%7B%22g%22%3A%22e3a19647-699c-e595-ba2f-d8a0654a2e14%22%2C%22c%22%3A1525430385018%2C%22l%22%3A1525430385018%7D; __ssid=35a0fbf6-9d19-4289-996c-0bfba46c026b; _omappvp=wWe6NRCEOaQW76xtU8lYAhNyQ83c86xtz3AufqEOoPbFWTLrGCAOMjcIyAIXudXtVhFwObcOGTbhrRbnSiBFk9Z1mxdan8uN; cto_lwid=54d4a608-82cb-4f31-9fd4-d52255987263; __zlcmid=mFhJbYVFxacoH1; appier_uid_2=yRlZz2OixSNUoKpocV5O6w; __zlcprivacy=1; _gid=GA1.2.925910841.1525969555; _atrk_ssid=l-QvgtIxWHyb3I7hxqS7jk; ab.storage.sessionId.b0a7b43e-b927-41a2-8f56-8183f42abb09=%7B%22g%22%3A%2205a932f9-e58c-abc1-8a26-7b5fcef60ffa%22%2C%22e%22%3A1526012004955%2C%22c%22%3A1526008764512%2C%22l%22%3A1526010204955%7D; appier_tp=; _atrk_sessidx=4; appier_pv_counternL1t5crDJLr4VSH=2; _uetsid=_uetf941f4d3",
        "Host": "www.honestbee.tw",
        "Pragma": "no-cache",
        "Referer": "https://www.honestbee.tw/zh-TW/groceries/stores/fresh-by-honestbee-tw?sort=price_desc",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36",
        "x-honestbee-cache": "enable"
    }''')

    resp = requests.get(url, headers=headers)
    data = resp.json()
    if data.get('products'):
        products = [{k.lower():v for k,v in row.items()} for row in data.get('products')]
        for row in products:
            row['brand_id'] = int(store_id)
        to_postgres.delay(products)
    #print(json.dumps(resp.json(), ensure_ascii=False))
    return (store_id, data)

@app.task
def get_total_pages(storeId_and_productData):
    store_id, product_data = storeId_and_productData
    return (store_id, product_data['meta']['total_pages'])

@app.task
def dispatch_get_products(storeId_and_totalPages):
    store_id, total_pages = storeId_and_totalPages
    if total_pages == 1:
        return
    for x in range(2, total_pages + 1):
        get_products.delay(store_id, x)

@app.task
def to_postgres(data):
    #print(data)
    models.Product.insert_many(data).execute()
    # except Exception as e:
    #     print(e)
    #     print(data)


if __name__ == '__main__':
    for store_id in get_stores():
        chain(get_products.s(store_id) |
                get_total_pages.s() |
                dispatch_get_products.s())()

