# coding: utf-8
import re

import requests

import products

HOST = 'https://www.honestbee.tw'
resp = requests.get(HOST + '/en/groceries/stores')
s = [HOST + h for h in re.findall('(/en/groceries/stores/.+?)"', resp.text)]

for store in s:
    print(store)
    products.get_product(store)
