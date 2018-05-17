# coding: utf-8
import os
import re
import json
import datetime

from celery import Celery, group, chain
import requests
import models
import peewee
from playhouse.shortcuts import model_to_dict

app = Celery('main', broker='pyamqp://guest@%s//'%(os.getenv('RBMQ_HOST')))
app.conf.task_routes = {
        'main.to_postgres': {'queue': 'to_postgres'},
        'main.to_es':       {'queue': 'to_es'}
        }
#brands_dict = {row.id: model_to_dict(row) for row in models.Brands}
HOST = 'https://www.honestbee.tw'
ES_HOST = os.getenv('ES_HOST')


#@app.task
def process_brands(brands):
    models.Brands.drop_table()
    models.Brands.create_table()
    (models.Brands
            .insert_many(brands)
            .execute())

def get_stores():
    """
    return [storeid<str>]
    """
    resp = requests.get(HOST + '/zh-tw/groceries/stores')
    window_data = json.loads(re.findall('window.__data=(.+?); window.__i18n', resp.text, re.DOTALL)[0])
    brands = window_data['groceries']['brands']['byId']
    brands = [{k.lower():v for k,v in row.items()} for kk, row in brands.items()]
    stores = [brand for brand in brands]
    process_brands(brands)
    #print("got brands")
    return stores

@app.task
def get_products(payload):
    department_name = payload['department_name']
    category_name = payload['category_name']
    store_name = payload['store_name']
    store_slug = payload['store_slug']
    department_id = payload['department_id']
    category_id = payload['category_id']
    store_id = payload['store_id']
    page = payload.get('page', 1)

    url = """https://www.honestbee.tw/api/api/departments/{department_id}?categoryIds%5B%5D={category_id}&sort=ranking&page={page}&storeId={store_id}&fields%5B%5D=categories""".format(
            department_id=department_id,
            category_id=category_id,
            store_id=store_id,
            page=page)

    headers = json.loads(r'''{
    "Accept": "application/vnd.honestbee+json;version=2",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-TW",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Cookie": "connect.sid=s%3A608o0mr0PaNmG0og-XBxSVwcYdlnbeht.Y5Ec5ICbb7NXCLmnODO1ASJc7JSNYQHg%2F3XXtZK1DY8; ajs_user_id=null; ajs_group_id=null; ajs_anonymous_id=%227c983032-927c-4214-8c4e-d306827ac44a%22; _ga=GA1.2.937310059.1526023442; ab.storage.deviceId.b0a7b43e-b927-41a2-8f56-8183f42abb09=%7B%22g%22%3A%222b83d6d3-2db7-27f2-4892-409d5139feae%22%2C%22c%22%3A1526023442611%2C%22l%22%3A1526023442611%7D; __ssid=34fc6072-d449-4851-aca0-a951b9e8bd7a; appier_utmz=%7B%7D; _atrk_siteuid=_nwI401QSL6fIj_O; _omappvp=E3IXL25nSz6PJX8mV07BQqTa2hXrMssF5zsp4CTEUnNFxeiN4HTnd3dVv1lsTFPoUS6JQ1xx3Zj5cmYOnf4Es3gzogQvSxI9; __zlcmid=mMhKeI2JX9LkJG; appier_uid_2=QA2V4hxPSE2lnYR44eXsv7; _gid=GA1.2.935251516.1526294893; cto_lwid=e5953c02-8ed8-4168-a931-f54031873972; _honestbees_session=V3JOaXFDWThDdWZtWi9jOVhZM3VpSER6RkFDdlBQUGREL1RadHJVaGlKZXNJY3MyTUZVTE8wUW1rMS9xaTAybllkK1ZMYzA4eThEeE12MFJxaWx3N2RWbzRUYldFUitvMDRRdHVmQTY4aTkzL3pVaGxMUHBmdlpRZkJHQW1vOWhaeTNWQ3NjMXZuRGZTeDRaWnpMcFFnPT0tLUJrdEg3aWFHNDNtcjFsdGZtM0ZEeHc9PQ%3D%3D--8894b18b1007ee1ecaebb802cfb37d2762c1e5b5; appier_tp=; _atrk_ssid=MQ4RcCMcUDfxz-8nJ_4zyw; appier_pv_counternL1t5crDJLr4VSH=0; _atrk_sessidx=2; _gat=1; _uetsid=_uetc15ffeeb; ab.storage.sessionId.b0a7b43e-b927-41a2-8f56-8183f42abb09=%7B%22g%22%3A%22dd0dc06d-d3ef-8b2f-0d41-e61f3fc28fc4%22%2C%22e%22%3A1526530172136%2C%22c%22%3A1526528372070%2C%22l%22%3A1526528372136%7D",
    "Host": "www.honestbee.tw",
    "Pragma": "no-cache",
    "Referer": "https://www.honestbee.tw/zh-TW/groceries/stores/american-wholesaler/departments/7313/categories/38037",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36",
    "x-honestbee-cache": "enable"
}''')
    #print(url)
    to_pg_data = []
    resp = requests.get(url, headers=headers)
    for product in resp.json().get('products'):
        product['store_name'] = store_name
        product['category_name'] = category_name
        product['department_name'] = department_name
        product['url'] = "https://www.honestbee.tw/zh-TW/groceries/stores/{store_slug}/products/{product_id}".format(
                        store_slug=store_slug,
                        product_id=product['id'])
        product['store_id'] = store_id
        product['category_id'] = category_id
        product['department_id'] = department_id
        product['product_id'] = product['id']
        product['dt'] = payload['dt']
        product = {k.lower():v for k,v in product.items()}
        to_pg_data.append(product)
    to_postgres.delay(to_pg_data)
    to_es.delay(to_pg_data)
    if int(resp.json()['meta']['current_page']) == 1 and int(resp.json()['meta']['total_pages']) > 1:
        print("%s,%s,%s"%(payload,int(resp.json()['meta']['current_page']),int(resp.json()['meta']['total_pages'])))
        for pg in range(2, int(resp.json()['meta']['total_pages']) + 1):
            payload['page'] = pg
            get_products.delay(payload)

@app.task
def dispatch_get_products(payload):
    """
    payload={
    'store_id': [store_id<str>],
    'dt': <str>,
    'process_department': {id: department{}}
    'process_category': {id: category{}}
    }
    """
    store = payload['store']
    for category_id, category in payload['process_category'].items():
        try:
            department = payload['process_department'][str(category['department_id'])]
        except:
            print("error, %s"%payload)
        payload2 = {}
        payload2['dt'] = payload['dt']
        payload2['department_id'] = department['id']
        payload2['department_name'] = department['name']
        payload2['store_id']     = department['store_id']
        payload2['store_name']   = store['name']
        payload2['store_slug']   = store['slug']
        payload2['category_id']  = category_id
        payload2['category_name'] = category['title']
        get_products.delay(payload2)

@app.task
def to_es(data_list):
    bulk_data = []
    for data in data_list:
        d = {
            "title": data['title'],
            "price": data['price'],
            "store_id": data['store_id'],
            "product_id": data['product_id'],
            "store_name": data['store_name'],
            "imageurl": data['imageurl'],
            "size": data['size'],
            "url": data.get('url'),
            "updated_at": data['dt'],
            "status": data['status'],
            "currency": data['currency']
        }
        bulk_data.append('''{ "index":{ "_id": "%s" } }'''%(data['id']))
        bulk_data.append('''%s'''%json.dumps(d))

    data = '\n'.join(bulk_data) + '\n'

    resp = requests.put('http://{}:9200/honestbee/main/_bulk'.format(ES_HOST), headers={'content-type': 'application/x-ndjson'},
            data=data)
    return resp.json()

# @app.task
# def to_es(data):
#     d = {
#         "title": data['title'],
#         "price": data['price'],
#         "store_id": data['store_id'],
#         "product_id": data['product_id'],
#         "imageurl": data['imageurl'],
#         "size": data['size'],
#         "url": data.get('url'),
#         "updated_at": data['dt'],
#         "status": data['status'],
#         "currency": data['currency']
#     }
#     resp = requests.put('http://{}:9200/honestbee/main/{}'.format(ES_HOST, data['product_id']), headers={'content-type': 'application/json'},
#             json=d)
#     return resp.json()

@app.task
def to_postgres(data):
    #print(data)
    try:
        with models.database.atomic():
            models.Product.insert_many(data).execute()
    except peewee.IntegrityError:
        return "duplicate"

    # except Exception as e:
    #     print(e)
    #     print(data)

@app.task
def store_department(department):
    del department['categories']
    models.Departments.create(**department)

@app.task
def process_department(payload):
    """
    payload={
    'store_id': [store_id<str>],
    'dt': <str>,
    'departments': [department{}],
    }
    payload={
    'store_id': [store_id<str>],
    'dt': <str>,
    'departments': [department{}],
    'process_department': {id: department{}}
    }
    """
    payload['process_department'] = {}
    for department in payload.get('departments'):
        department = {k.lower():v for k,v in department.items()}
        department['store_id'] = int(payload.get('store_id'))
        store_department.delay(department)
        department['id'] = int(department['id'])
        payload['process_department'][int(department['id'])] = department
    return payload

@app.task
def store_category(category):
    models.Categories.create(**category)

@app.task
def process_category(payload):
    """
    payload={
    'store_id': [store_id<str>],
    'dt': <str>,
    'departments': [department{}],
    'process_department': {id: department{}}
    }
    payload={
    'store_id': [store_id<str>],
    'dt': <str>,
    'process_department': {id: department{}}
    'process_category': {id: category{}}
    }
    """
    payload['process_category'] = {}
    for department in payload.get('process_department').values():
        # print(department)
        for category in department.get('categories'):
            category = {k.lower():v for k,v in category.items()}
            category['department_id'] = int(department['id'])
            store_category.delay(category)
            category['id'] = int(category['id'])
            payload['process_category'][int(category['id'])] = category
    del payload['departments']
    return payload

@app.task
def get_directory(payload):
    """
    payload={
    'store_id': [store_id<str>],
    'dt': <str>
    }
    payload={
    'store_id': [store_id<str>],
    'dt': <str>,
    'departments': []
    }
    """
    store_id = int(payload['store_id'])
    dt       = payload['dt']

    url = "https://www.honestbee.tw/api/api/stores/{}/directory".format(store_id)
    headers = json.loads(r'''{
        "Accept": "application/vnd.honestbee+json;version=2",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-TW",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Cookie": "connect.sid=s%3A608o0mr0PaNmG0og-XBxSVwcYdlnbeht.Y5Ec5ICbb7NXCLmnODO1ASJc7JSNYQHg%2F3XXtZK1DY8; ajs_user_id=null; ajs_group_id=null; ajs_anonymous_id=%227c983032-927c-4214-8c4e-d306827ac44a%22; _ga=GA1.2.937310059.1526023442; ab.storage.deviceId.b0a7b43e-b927-41a2-8f56-8183f42abb09=%7B%22g%22%3A%222b83d6d3-2db7-27f2-4892-409d5139feae%22%2C%22c%22%3A1526023442611%2C%22l%22%3A1526023442611%7D; __ssid=34fc6072-d449-4851-aca0-a951b9e8bd7a; appier_utmz=%7B%7D; _atrk_siteuid=_nwI401QSL6fIj_O; _omappvp=E3IXL25nSz6PJX8mV07BQqTa2hXrMssF5zsp4CTEUnNFxeiN4HTnd3dVv1lsTFPoUS6JQ1xx3Zj5cmYOnf4Es3gzogQvSxI9; __zlcmid=mMhKeI2JX9LkJG; appier_uid_2=QA2V4hxPSE2lnYR44eXsv7; _gid=GA1.2.935251516.1526294893; cto_lwid=e5953c02-8ed8-4168-a931-f54031873972; _atrk_ssid=YpAsjIrqnZwwCSTMIRBmdj; appier_tp=; appier_pv_counternL1t5crDJLr4VSH=4; _uetsid=_uet85e08d3a; ab.storage.sessionId.b0a7b43e-b927-41a2-8f56-8183f42abb09=%7B%22g%22%3A%22ad61c68d-563e-54f5-2e47-c91611935c55%22%2C%22e%22%3A1526442516340%2C%22c%22%3A1526440025884%2C%22l%22%3A1526440716340%7D; _atrk_sessidx=15; _honestbees_session=S3oyaERudzN2SVhHOXBzandHWkhoOUZOd3F4YjFWbU85Nkx5RzZ3NTlnNkU3RTdVSzAzaE1oeFFqbmZJYllPREpBMUc1TFdNVGhWWjBXeTFpTkVlOTVOTUdjS2g4RnBGbGVOaEVMOTJRZnY3blJUNEdaUU4wbFNQaDJucS83MFFLTTIzOXVEbkkvU1ZaeEtGVW5MU29nPT0tLWZjMjExT09FaWVnN2xSVzNUQUl4Smc9PQ%3D%3D--ea719a7aa991c288bc691711fcba6ab95a4fb559",
        "Host": "www.honestbee.tw",
        "Pragma": "no-cache",
        "Referer": "https://www.honestbee.tw/zh-TW/groceries/stores/carrefour/departments/20272/categories/136156",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36",
        "x-honestbee-cache": "enable"
    }''')

    resp = requests.get(url, headers=headers)
    payload['departments'] = resp.json().get('departments')
    return payload

if __name__ == '__main__':
    models.Departments.drop_table()
    models.Departments.create_table()
    models.Categories.drop_table()
    models.Categories.create_table()
    dt = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).astimezone(datetime.timezone(datetime.timedelta(hours=8))).isoformat()
    for store in get_stores():
        payload = {}
        store_id = store['storeid']
        # chain(get_products.s((store_id,dt)) |
        #         get_total_pages.s() |
        #         dispatch_get_products.s())()
        payload['store_id'] = int(store_id)
        payload['store'] = store
        payload['dt'] = dt
        chain(get_directory.s(payload) |
                process_department.s() |
                process_category.s() |
                dispatch_get_products.s()
                )()

