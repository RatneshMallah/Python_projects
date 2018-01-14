import requests
import shutil
import os
import time
from bs4 import BeautifulSoup
from selenium import webdriver
#from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

capabilities = DesiredCapabilities.FIREFOX.copy()
capabilities['marionette'] = False
driver = webdriver.Firefox(capabilities=capabilities)

url = input("type url : ")#"http://www.planwallpaper.com/wallpaper-hd#"

"""
web_r = requests.get(url)
web_soup = BeautifulSoup(web_r.text, 'html.parser')
print(web_soup.findAll('img'))
"""
iterations = 0
while iterations < 10:
	driver.get(url)
	html = driver.execute_script("return document.documentElement.outerHTML")
	sel_soup = BeautifulSoup(html, 'html.parser')
	print(len(sel_soup.findAll("img")))
	images = []
	for i in sel_soup.findAll('img'):
		src = i['src']
		images.append(src)
	print(images)
	current_path = os.getcwd()
	for img in images:
		try:
			file_name = os.path.basename(img)
			img_r = requests.get(img, stream=True)
			new_file = os.path.join(current_path, 'images', file_name)
			with open(new_file, 'wb') as output_file:
				shutil.copyfileobj(img_r.raw, output_file)
			del img_r
		except:
			pass
	iterations += 1
	time.sleep(5)
