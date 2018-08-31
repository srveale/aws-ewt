from bs4 import BeautifulSoup
import requests
import time
import os
import uuid
import boto3

def lambda_handler(err, result):
	client = boto3.client('dynamodb')
	logs = [];

	for i in xrange(5):
		logs += getPageData(os.environ['page{}'.format(i+1)])

	for log in logs:
		client.put_item(TableName = 'ewt', Item=log)
		time.sleep(1) #Sleep to reduce writes per second

	return logs

def getPageData(page):
	page = requests.get(page);
	soup = BeautifulSoup(page.content, 'html.parser')

	hospitalElements = soup.findAll("a", {"class": "publicRepacSiteText"})
	hospitalLogs = [formatLog(hospitalElement) for hospitalElement in hospitalElements]

	return hospitalLogs;

def formatLog(hospitalElement):
	digits = [getDigits(digitOrder, hospitalElement)[0] for digitOrder in ['One', 'Two', 'Three', 'Four']]
	name = hospitalElement.getText()
	waitTimeString = "{}{}:{}{}".format(*digits)
	
	# convert to minutes
	try: 
		waitTime = int(digits[0]) * 600 + int(digits[1]) * 60 + int(digits[2]) * 10 + int(digits[3])
		print(waitTime)
	except TypeError:
		waitTime = -1

	return {
		'id': {'S': str(uuid.uuid1())}, 
		'hospitalName': {'S': name}, 
		'waitTimeString': {'S': waitTimeString},
		'waitTime': { 'N': str(waitTime) },
		'epochTime': {'N': str(time.time())}, #Why, AWS? why do you want a string for a number?
		'date': {'S': time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime())} 
	}

def getDigits(digitOrder, hospitalElement):
	gifs = hospitalElement.findParent("tr").findChildren("div", {"class": "publicClockNumber{}Gif".format(digitOrder)})

	if len(gifs) > 0:
		return [gif.findChildren("img")[0]['alt'][0] for gif in gifs]
	else:
		return [[], [], [], []]
