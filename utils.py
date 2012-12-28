import re,string

UNK, USA = range(2)
months = {'Jan': 1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}

# Parses the time period from the filename
def parseTimePeriod(filename):
	import datetime as dt
	raw_date = filename.split('.')[1]
	date, time = raw_date.split('_')
	date = [int(i) for i in date.split('-')]
	time = [int(i) for i in time.split('-')]
	return (dt.datetime(date[0], date[1], date[2], time[0], time[1]) + dt.timedelta(hours=5))
				
def assignBounds(location):
	if not location['shape']:
		lat = location['lat']
		lng = location['lng']
		if lat <= 48 and lat >= 25 and lng >= -125 and lng <= -66:
			return USA
		else:
			return UNK
	return False

# Need to make tests for this function
def wordFilter(words):

	punc = re.compile('[%s]'%re.escape(string.punctuation))
	num = re.compile('[%s]'%re.escape(string.digits))
	alpha = re.compile('[^a-z]')
	white = re.compile('[\s]')
	keywords = []
	file = open("stopwords//lextek.txt", 'r')
	stopwords = file.read().split()
	file.close()

	for word in words:
		# ignore long strings
		if len(word) > 20:
			continue

		# ignore url
		if u'http' in word:
			continue

		# ignore mentions
		if word[0] == '@':
			continue
		
		temp_word = punc.sub('',word)
		temp_word = num.sub('',temp_word)
		
		# ignore unicode
		if re.search(alpha, temp_word) != None:
			continue
		
		# ignore stopwords
		try:
			if temp_word in stopwords:
				continue
		except UnicodeWarning:
			print temp_word

		# ignore empty string
		if len(temp_word) == 0:
			continue
		if '\x00' in temp_word:
			temp_word = string.replace(temp_word, '\x00', '')
			print temp_word
		
		keywords.append(temp_word)
	return keywords
