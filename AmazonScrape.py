"""
PeerRoth, 12/2018, Help: scrapehero.com, pythonprogramming.net
Dependencies:
    lxml
    requests
    MySQLdb
"""
import csv, os, json
from itertools import cycle
from lxml import html  
import MySQLdb
import requests
from time import sleep
import time
from types import *

   
def insertSQL(details):
    #arguments: (#Your Server, #Your Server Login, #Your Server Password, #Your Database Name)
    conn = MySQLdb.connect("localhost", "root", "password", "amazon")
    c = conn.cursor()
    d = details.get("CATEGORY", "none")
    e = details.get("ORIGINAL_PRICE", "none")
    if type(e) is StringType:
        e = e
    else:
        e = 'N/A'
    f = details.get("NAME", "none")
    g = details.get("URL", "none")
    h = details.get("SALE_PRICE", "none")
    if type(h) is StringType:
        h = h
    else:
        h = 'N/A'
    i = details.get("AVAILABILITY", "none")
    #Strip series of \n's and long whitespaces
    i = i.replace('\n','').replace('          ','')
    c.execute("INSERT INTO scrape (CATEGORY,ORIGINALPRICE,NAME,URL,SALEPRICE,AVAILABILITY) VALUES (%s,%s,%s,%s,%s,%s)",
            (d,e,f,g,h,i))
    conn.commit()
    rows = c.fetchall()
 
def AmzonParser(url):
    #Proxies are from free proxy site.  There are a number of them accessible with a quick Google search.
    proxies = ['54.236.44.224:3128','205.177.86.213:8888','35.240.29.142:3128','173.192.21.89:8123']
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36'}
    proxy_pool = cycle(proxies)
    for i in range(1,6):
        #Get a proxy from the pool
        proxy = next(proxy_pool)
        try:
            page = requests.get(url, proxies={"https":proxy}, headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36"})
            while True:
                sleep(3)
                try:
                    doc = html.fromstring(page.content)
                    XPATH_NAME = '//h1[@id="title"]//text()'
                    XPATH_SALE_PRICE = '//span[contains(@id,"ourprice") or contains(@id,"saleprice")]/text()'
                    XPATH_ORIGINAL_PRICE = '//td[contains(text(),"List Price") or contains(text(),"M.R.P") or contains(text(),"Price")]/following-sibling::td/text()'
                    XPATH_CATEGORY = '//a[@class="a-link-normal a-color-tertiary"]//text()'
                    XPATH_AVAILABILITY = '//div[@id="availability"]//text()'
 
                    RAW_NAME = doc.xpath(XPATH_NAME)
                    RAW_SALE_PRICE = doc.xpath(XPATH_SALE_PRICE)
                    RAW_CATEGORY = doc.xpath(XPATH_CATEGORY)
                    RAW_ORIGINAL_PRICE = doc.xpath(XPATH_ORIGINAL_PRICE)
                    RAw_AVAILABILITY = doc.xpath(XPATH_AVAILABILITY)
 
                    NAME = ' '.join(''.join(RAW_NAME).split()) if RAW_NAME else None
                    SALE_PRICE = ' '.join(''.join(RAW_SALE_PRICE).split()).strip() if RAW_SALE_PRICE else None
                    CATEGORY = ' > '.join([i.strip() for i in RAW_CATEGORY]) if RAW_CATEGORY else None
                    ORIGINAL_PRICE = ''.join(RAW_ORIGINAL_PRICE).strip() if RAW_ORIGINAL_PRICE else None
                    AVAILABILITY = ''.join(RAw_AVAILABILITY).strip() if RAw_AVAILABILITY else None
 
                    if not ORIGINAL_PRICE:
                        ORIGINAL_PRICE = SALE_PRICE
 
                    if page.status_code!=200:
                        raise ValueError('captha')
                    data = {
                    'NAME':NAME,
                    'SALE_PRICE':SALE_PRICE,
                    'CATEGORY':CATEGORY,
                    'ORIGINAL_PRICE':ORIGINAL_PRICE,
                    'AVAILABILITY':AVAILABILITY,
                    'URL':url,
                    }
                    insertSQL(data)
                    #Return data for other uses besides SQL insert
                    return data
                except Exception as e:
                    print('Exception: ', e)
        except Exception as e:
            #Most free proxies will often get connection errors. You will have retry the entire request using another proxy to work. 
            #We will just skip retries as its beyond the scope of this tutorial and we are only downloading a single url 
            print(e,"Skipping. Connnection error")    
             
def ReadAsin():
    # AsinList = csv.DictReader(open(os.path.join(os.path.dirname(__file__),"Asinfeed.csv")))
    AsinList = [
    'B00O9A48N2',
    'B0046UR4F4',
    'B00JGTVU5A',
    'B00GJYCIVK',
    'B00EPGK7CQ',
    'B00EPGKA4G',
    'B00YW5DLB4',
    'B00KGD0628']
    extracted_data = []
    
    for i in AsinList:
        print('i')
        url = "http://www.amazon.com/dp/"+i
        extracted_data.append(AmzonParser(url))
        sleep(5)
 
 
if __name__ == "__main__":
    ReadAsin()
