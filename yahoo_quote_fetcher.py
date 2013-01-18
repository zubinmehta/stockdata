#!/usr/bin/python
# coding: utf-8

import requests
from bs4 import BeautifulSoup
import re
import redis
import gevent
from gevent import monkey
import pandas
from gevent.pool import Pool

r = redis.Redis(db=2)
monkey.patch_all(thread=False)

"""
url_template = "http://in.finance.yahoo.com/lookup/stocks;_ylt=Aoxoscj0RkThPbCCsexH2yV9UblG;_ylu=X3oDMTFiM3RzMzF1BHBvcwMyBHNlYwN5ZmlTeW1ib2xMb29rdXBSZXN1bHRzBHNsawNzdG9ja3M-?s=.NS&t=S&m=ALL&r=&b=%s"

def do_stuff(url):
    print url
    st = requests.get(url)
    soup = BeautifulSoup(st.content)
    stocks = soup.find_all(attrs={"class":re.compile("yui-dt-.*")})
    for stock_data in stocks:
        a = stock_data
        stock_name = a.td.getText()
        if a.td.find_next_siblings()[3].text == "NSI":
            r.rpush("yahoo-stocks", stock_name)

jobs = [gevent.spawn(do_stuff, url_template %(i*20)) for i in range(20) ]
gevent.joinall(jobs)
"""

eqs = pandas.read_csv("eqs.csv")
df = eqs.ix[eqs.ix[:, 2] == "EQ"]

# 1. get the list of equities from the nse website csv link
# 2. create the 9 + .NS name to query yahoo
# 3. write the data querier and saver for 1 yahoo stock

def fetch_yahoo(yahoo_stock_name, folder="ynse"):
    print yahoo_stock_name
    url = "http://ichart.finance.yahoo.com/table.csv?s=%s&a=01&b=12&c=1992&d=04&e=27&f=2012&g=d&ignore=.csv" %yahoo_stock_name
    print url
    f = open("%s/%s" %(folder, yahoo_stock_name), "wb+")
    f.write(requests.get(url).content)
    f.close()

if __name__ == "__main__":
    pool = Pool(10)
    jobs = [pool.spawn(fetch_yahoo, "%s.NS" %j[0:9]) for j in df.SYMBOL]
    pool.join()
