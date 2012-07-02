# Tweet Indexer
#
# Author: Jason Bolden
# Date: June 15, 2012
#
#

from pymongo import *
from datetime import *
import json

DEBUGGING = False
REINDEX = False
months = {'Jan': 1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}

def WordFilter(words):
	import re
	punc = re.compile('[%s]'%re.escape(string.punctuation))
	
	keywords = []
	file = open("stopwords\\lextek.txt", 'r')
	stopwords = file.read().split()
	file.close()
	
	for word in words:
		# ignore unicode
		if '\u' in word:
			continue
			
		# ignore mentions
		if word[0] == '@':
			continue
		# ignore stopwords
		if word in stopwords:
			continue
			
		temp_word = regex.sub('',word)
	
		
class Tweet:
	def __init__(self, tweet):
		global DEBUGGING
		try:
			# Information about the tweet
			self.id = tweet['id']
			self.retweet_count = tweet['retweet_count']
			self.contents = tweet['text'].lower()
			self.keywords = list(set(word_filter(self.contents.split())))
			
			self.keyword_counts = {}
			filtered_words = word_filter(self.contents.split())
			for word in self.keywords:
				self.keyword_counts[word] = filtered_words.count(word)
				
			self.urls = []
			self.user_mentions = []
			self.hashtags = []
			
			for url in tweet['entities']['urls']:
				self.urls.append(url['expanded_url']
			for mention in tweet['entities']['user_mentions']:
				self.user_mentions.append(mention['id'])
			for tag in tweet['entities']['hashtags']:
				self.hashtags.append(tag['text'])
			
			#This will be a date object
			tokens = tweet['created_at'].split(' ')
			time = tokens[3].split(':')
			self.date = datetime(int(tokens[5]), months[tokens[1]], int(tokens[2]), int(time[0]), int(time[1]), int(time[2]))
			
			if tweet['coordinates']:
				self.location = {'type': tweet['coordinates']['type'], 'shape': None, 'lat': tweet['coordinates']['coordinates'][1], 'lng': tweet['coordinates']['coordinates'][0]}
			elif tweet['place']:
				shape = []
				for coord in tweet['place']['coordinates']:
					shape.append({'lat': coord[1], 'lng': coord[0]})
				self.location = {'type': tweet['place']['type'], 'shape': shape, 'lat': None, 'lng': None }
			else:
				self.valid = False
				
				
			# Information about the user
			self.user = tweet['user']['id']
			self.follower_count = tweet['user']['follower_count']
			
				
			
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
		O_index.ensure_index('keyword', unique=True)
		O_index.ensure_index('date')
		O_index.reindex()
		C_index.ensure_index('date', unique=True)
		C_index.reindex()

# Structure {word: count, ...}
keyword_occurences = {}
total_keywords = 0
		
def PopulateDB(tweets):
	global DK_index, H_index, keyword_occurrences, total_keywords
	counter = 0
	loaded = 0
	duplicate = 0
	for tweet in tweets:
		counter += 1
		if DK_index.find({'_id':tweet.id}).count() == 0:
			DK_index.insert(tweet.toDictionary())
			#print "Tweet id:" + str(tweet.id) + " loaded into DateKeywordCollection."
			loaded += 1
		else:
			duplicate += 1
			if DEBUGGING:
				print "Tweet id:" + str(tweet.id) + " already loaded into DateKeywordCollection, skipping."
		if H_index.find({'_id':tweet.id}).count() == 0:
			H_index.insert(tweet.toDictionary())
			#print "Tweet id:" + str(tweet.id) + " loaded into HashtagCollection."
		else:
			if DEBUGGING:
				print "Tweet id: " + str(tweet.id) + " already loaded into HashtagCollection, skipping."
		if counter%1000 == 0:
			print '\n'
			print str(counter) + " out of " + str(len(tweets)) + " completed."
			print str(loaded) + " tweets have been loaded."
			print str(duplicate) + " tweets were already loaded."
		
		for word, count in tweet.keyword_counts.items():
			if word not in keyword_occurrences.keys():
				keyword_occurrences[word] = count
			else:
				keyword_occurrences[word] += count
			total_keywords += count
				
def LoadTweets(filenames):
	InitDB()
	counter = 0
	tokens = filenames[0].split('.')
	tokens = tokens[1].split('_')
	tokens = tokens[0].split('-')
	Date = datetime(int(tokens[0]), int(tokens[1]), int(tokens[2]))
	for filename in filenames:
		if filename[-1] == 'z':
			continue
		tweets = []
		file = open(filename)
		print "Loading: " + filename
		# List of tweet objects
		keywords
		for line in file:
			try:
				temp = Tweet(json.loads(line))
				if temp.valid:
					tweets.append(temp)
			except:
				if DEBUGGING:
					print "Invalid json: " + line
					continue
		file.close()
		PopulateDB(tweets)
		log = open("log", 'a')
		log.write(filename + " loaded.\n")
		log.close()
		counter += 1
		print str(counter) + " out of " + str(len(filenames)) + " loaded."
	
	import json
	file = open(str(date)+".txt", 'w')
	file.write(str(total_keywords) + '\n')
	file.write(json.dumps(keyword_occurrences, sort_keys=True, indent=4))
	file.close()
	
	temp = dict(('date', date) + ('keyword_counts', keyword_occurrences))
	O_index.insert(temp)
	temp = dict(('date', date) + ('total_keywords', total_keywords))
	C_index.insert(temp)
	
# Calculates the Document frequencies of all hashtags that appear with the query
def CountHashtags(tweets):
	dfs = {}
	for tweet in tweets:
		# Check to see if the tag is already in the dictionary
		# Loop through all tweets to count the number of occurrences
		# calculate the df
		for hashtag in tweet.hashtags:
			if hashtag in dfs.keys():
				continue
			occurrences = 0.0
			for t in tweets:
				if t.valid and hashtag in t.hashtags:
					occurrences += 1
			#print hashtag + ": " + str(occurrences)
			dfs[hashtag] = (occurrences/len(tweets), hashtag)
	return dfs

if __name__ == '__main__':
	import os, sys
	if sys.argv[1] == 'True':
		DEBUGGING = True
	filenames = []
	for path, names, files in os.walk('.//tweets//'+sys.argv[2]):
		for file in files:
			filenames.append(os.path.join(path, file))
	LoadTweets(filenames)
