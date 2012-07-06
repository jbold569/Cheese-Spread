# Tweet Indexer
#
# Author: Jason Bolden
# Date: June 15, 2012
#
#

from pymongo import *
from datetime import *
import Probe
import json
import re, string

DEBUGGING = False
probe = Probe.Probe()
REINDEX = False
months = {'Jan': 1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}

def WordFilter(words):
	
	punc = re.compile('[%s]'%re.escape(string.punctuation))
	num = re.compile('[%s]'%re.escape(string.digits))
	white = re.compile('[\s]')
	keywords = []
	file = open("stopwords//lextek.txt", 'r')
	stopwords = file.read().split()
	file.close()
	
	for word in words:
		probe.StartTiming("wordFilter")
		# ignore unicode
		try:
			word.encode('ascii')
		except UnicodeEncodeError:
			continue
			
		# ignore mentions
		if word[0] == '@':
			continue
		temp_word = punc.sub('',word)
		temp_word = num.sub('',temp_word)
		
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
			print temp_word
			
		keywords.append(temp_word)
		probe.StopTiming("wordFilter")
		
	return keywords
		
class Tweet:
	def __init__(self, tweet):
		global DEBUGGING
		try:
			# Information about the tweet
			self.id = tweet['id']
			self.retweet_count = tweet['retweet_count']
			self.contents = tweet['text'].lower()
			self.keywords = list(set(WordFilter(self.contents.split())))
			
			self.keyword_counts = {}
			filtered_words = WordFilter(self.contents.split())
			for word in self.keywords:
				self.keyword_counts[word] = filtered_words.count(word)
				
			self.urls = []
			self.user_mentions = []
			self.hashtags = []
			
			for url in tweet['entities']['urls']:
				self.urls.append(url['expanded_url'])
			for mention in tweet['entities']['user_mentions']:
				self.user_mentions.append(mention['id'])
			for tag in tweet['entities']['hashtags']:
				self.hashtags.append(tag['text'])
			
			#This will be a date object
			tokens = tweet['created_at'].split(' ')
			time = tokens[3].split(':')
			self.date = datetime(int(tokens[5]), months[tokens[1]], int(tokens[2]), int(time[0]), int(time[1]), int(time[2]))
			self.valid = True
			
			if tweet['coordinates']:
				if DEBUGGING:
					print "Exact location."
				self.location = {'type': tweet['coordinates']['type'], 'shape': None, 'lat': tweet['coordinates']['coordinates'][1], 'lng': tweet['coordinates']['coordinates'][0]}
			elif tweet['place']:
				if DEBUGGING:
					print "Bounding Box"
				shape = []
				for coord in tweet['place']['bounding_box']['coordinates'][0]:
					shape.append({'lat': coord[1], 'lng': coord[0]})
				self.location = {'type': tweet['place']['bounding_box']['type'], 'shape': shape, 'lat': None, 'lng': None }
			else:
				self.valid = False
				
				
			# Information about the user
			self.user = tweet['user']['id']
			self.follower_count = tweet['user']['followers_count']
			
				
			
		except KeyError as e:
			self.valid = False
			if DEBUGGING:
				print "Missing data from tweet: " + str(e)
				print tweet.keys()		
	
	def __str__(self):
		return "Location type: {0} at ({1}, {2})".format(self.location['type'], self.location['lat'], self.location['lat'])
	
	# This function returns a list of dictionaries of the same tweet on with
	# different keywords
	def toDictionary(self):
		temp_dict = {
			'_id' : self.id,
			'contents' : self.contents,
			'hashtags' : self.hashtags,
			'date' : self.date,
			'location' : self.location,
			'valid' : self.valid,
			'keywords' : self.keywords,
			'urls': self.urls,
			'retweet_count': self.retweet_count,
			'user_mentions': self.user_mentions,
			'user': self.user,
			'follower_count': self.follower_count
		}
			
		return temp_dict

DK_index = None
H_index = None
O_index = None
C_index = None
		
def InitDB():
	global DK_index, H_index, O_index, C_index
	connection = Connection()
	print "Connecting to DB"
	db = connection.GeoTaggedTweets
	
	DK_index = db.DateKeywordCollection
	H_index = db.HashtagCollection
	O_index = db.OccurrencesCollection
	C_index = db.KeywordCountCollection
	
	if REINDEX:
		DK_index.ensure_index('date')
		DK_index.ensure_index('keywords')
		DK_index.reindex()
		H_index.ensure_index('hashtags')
		H_index.reindex()
		O_index.ensure_index('keyword')
		O_index.ensure_index('date', unique=True)
		O_index.reindex()
		C_index.ensure_index('date', unique=True)
		C_index.reindex()

# Structure {word: count, ...}
keyword_occurrences = {}
total_keywords = 0
		
def PopulateDB(tweet, stats):
	global DK_index, H_index, keyword_occurrences, total_keywords
	counter = stats[0] + 1
	loaded = stats[1]
	duplicate = stats[2]
	
	if DK_index.find({'_id':tweet.id}).count() == 0:
		probe.StartTiming("dbInserts")
		DK_index.insert(tweet.toDictionary())
		probe.StopTiming("dbInserts")
		#print "Tweet id:" + str(tweet.id) + " loaded into DateKeywordCollection."
		loaded += 1
	else:
		duplicate += 1
		if DEBUGGING:
			print "Tweet id:" + str(tweet.id) + " already loaded into DateKeywordCollection, skipping."
	if H_index.find({'_id':tweet.id}).count() == 0:
		probe.StartTiming("dbInserts")
		H_index.insert(tweet.toDictionary())
		probe.StopTiming("dbInserts")
		#print "Tweet id:" + str(tweet.id) + " loaded into HashtagCollection."
	else:
		if DEBUGGING:
			print "Tweet id: " + str(tweet.id) + " already loaded into HashtagCollection, skipping."
	
	for word, count in tweet.keyword_counts.items():
		probe.StartTiming("dictInserts")
		if word not in keyword_occurrences.keys():
			keyword_occurrences[word] = count
		else:
			keyword_occurrences[word] += count
		total_keywords += count
		probe.StopTiming("dictInserts")
		
		'''probe.StartTiming("fileOutDict")
		file = open("out.txt", 'w')
		file.write(str(total_keywords) + '\n')
		file.write(json.dumps(keyword_occurrences, sort_keys=True, indent=4))
		file.close()
		probe.StopTiming("fileOutDict")
		'''
	return counter,loaded,duplicate

def LoadTweets(path, filenames):
	InitDB()
	file_counter = 0
	print len(filenames)
	tokens = filenames[0].split('.')
	tokens = tokens[1].split('_')
	tokens = tokens[0].split('-')
	Date = datetime(int(tokens[0]), int(tokens[1]), int(tokens[2]))
	
	for filename in filenames:
		if filename[-1] == 'z':
			continue
		file = open(path+filename)
		print "Loading: " + filename
		# line num, loaded tweets, duplicate tweets
		stats = (0,0,0)
		for line in file:
			#try:
			temp = Tweet(json.loads(line))
			if temp.valid:
				probe.StartTiming("parsedLine")
				stats = PopulateDB(temp, stats)
				probe.StopTiming("parsedLine")
			#except :
			#	if DEBUGGING:
				#	print "Invalid json: " + line
				#	continue
			
			if stats[0]%1000 == 0:
				print '\n'
				print str(stats[0]) + " tweets completed."
				print str(stats[1]) + " tweets have been loaded."
				print str(stats[2]) + " tweets were already loaded."
			
	
		file.close()
		print "File loaded and Tweet objects inserted."
		log = open("log", 'a')
		log.write(filename + " loaded.\n")
		log.close()
		file_counter += 1
		print str(file_counter) + " out of " + str(len(filenames)) + " loaded."
		
	global keyword_occurrences, total_keywords
	#for key in keyword_occurrences.keys(): 
		#print key + " " + str(len(key))
	file = open(str(Date).split()[0]+".txt", 'w')
	file.write(str(total_keywords) + '\n')
	file.write(json.dumps(keyword_occurrences, sort_keys=True, indent=4))
	file.close()
	
	O_index.insert({'date': Date, 'keyword_counts': keyword_occurrences})
	C_index.insert({'date': Date, 'total_keywords': total_keywords})

if __name__ == '__main__':
	import os, sys
	if sys.argv[1] == 'True':
		DEBUGGING = True
	if sys.argv[2] == 'True':
		REINDEX = True
	
	probe.InitProbe("parsedLine", "%.3f tweets parsed a second.", 10)
	probe.InitProbe("dbInserts", "%.3f database inserts a second.", 10)
	probe.InitProbe("dictInserts", "%.3f keyword dicitonary inserts a second.", 10)
	probe.InitProbe("wordFilter", "%.3f keywords filtered a second.\n", 10)
	probe.RunProbes()
		
	filenames = []
	path = './/tweets//'+sys.argv[3]+'//'
	filenames = os.listdir(path)
	LoadTweets(path, filenames)
