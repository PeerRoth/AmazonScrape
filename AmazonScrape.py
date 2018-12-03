""" 
Tool to scrape Amazon for product details and insert them
into a database via MySQL.
-Enjoy!!!

PeerRoth, 12/2018, Help: scrapehero.com, pythonprogramming.net

Dependencies:
    lxml
    requests
    MySQLdb

Futures:
    Database and table creation
    Version compatibility
    Develop an ASIN generator
"""

import csv,os,json
from itertools import cycle
from lxml import html
import MySQLdb
import requests
from time import sleep
from types import *


class DataBaseObject(object):
	""" Create a Dictionary with MySQL login and database name """
	def __init__(self, mysql_id, mysql_password, database_name):
		self.login = mysql_id
		self.password = mysql_password
		self.dbname = database_name
	
	def getCreds(self):	
		return {'li':self.login, 'pw':self.password, 'db':self.dbname}

class DataBaseInsert(object):
	""" Insert product data into SQL database on server. """
	def __init__(self, log):
		""" Grab login, password and database name from DBO """
        self.dbname = log.get('db', None)
		self.login = log.get('li', None)
		self.password = log.get('pw', None)
	
	def insertSQL(self, details):
		""" Grab desired values from DataGrabber Dictionary """
		conn = MySQLdb.connect("localhost", self.login, self.password, self.dbname)
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
		# Strip newlines and long white spaces which frequently occur if no availabilty
        i = i.replace('\n','').replace('		  ','')
        # These columns were already created by user
		c.execute("INSERT INTO scrape (CATEGORY,ORIGINALPRICE,NAME,URL,SALEPRICE,AVAILABILITY) VALUES (%s,%s,%s,%s,%s,%s)",
				(d,e,f,g,h,i))
		conn.commit()
		rows = c.fetchall()

class DataGrabber(object):
	""" Grab desired attributes from Amazon product """
	def __init__(self, asin_file=None, proxy_list=None, log=None):
		""" asin_file arg must be a csv file with only asin ids with no quotations or commas """
		self.asin_list = []
		if asin_file:
			with open(asin_file) as asin_file_open:
				print('open')
				read_file = csv.DictReader(asin_file_open)
				print('readfile',read_file)
				for line in read_file:
					self.asin_list.append(line['asin'])
		else:
		    # This is a backup list with random products
            self.asin_list = [
			'B00O9A48N2',
			'B0046UR4F4',
			'B00JGTVU5A',
			'B00GJYCIVK',
			'B00EPGK7CQ',
			'B00EPGKA4G',
			'B00YW5DLB4',
			'B00KGD0628']
            
		if log:
			self.log = log 
		 
	def AmazonGet(self, url, proxies=None):
		""" Assign proxy, scrape data, append to Dictionary """
        if proxies:
			self.proxies = proxies
		else:
			# Backup list of proxies that worked on 12.2.18
            self.proxies = ['54.236.44.224:3128','205.177.86.213:8888','35.240.29.142:3128','173.192.21.89:8123']
		headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36'}
		proxy_pool = cycle(self.proxies)
		
        for i in range(1,6):
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
							raise ValueError('You\'re caught.  Get new proxies, check if headers updated.')
						
                        data = {
						'NAME': NAME,
						'SALE_PRICE': SALE_PRICE,
						'CATEGORY': CATEGORY,
						'ORIGINAL_PRICE': ORIGINAL_PRICE,
						'AVAILABILITY': AVAILABILITY,
						'URL': url}
						
                        dbinsert = DataBaseInsert(self.log)
						dbinsert.insertSQL(data)
						return data
					except Exception as e:
						print('e', e)
			
			except Exception as e:
				# Some free proxies will often get connection errors.
				print(e, "Skipping proxy. Connnection error")
	
	def readAsin(self):
		""" Send elements from list of ASIN's to AmazonGet(). """
        self.AsinList = self.asin_list
        
        for i in self.AsinList:
			url = "http://www.amazon.com/dp/" + i
			self.AmazonGet(url)
			sleep(5)
		
 
if __name__ == "__main__":
	ASIN_Feeder = 	'asinlist.csv'  # FILL IN FILE CONTAINING ASINS
	Database_Name = 'amazon'		# FILL IN NAME OF DATABASE'
	Table_Name = 	'scrape'		# FILL IN NAME OF TABLE WITHIN DATABASE'
	Login_ID = 		'root'			# FILL IN YOUR MYSQL USERNAME'
	Login_PW = 		'*************' # FILL IN YOUR MYSQL PASSWORD'
	
	one = DataBaseObject(Login_ID, Login_PW, Database_Name)
	two = DataGrabber(asin_file='asinlist.csv', log=one.getCreds())
	two.readAsin()
    
