# Tweet Indexer
#
# Author: Jason Bolden
# Date: June 15, 2012
#
#

from pymongo import *
from datetime import *
import json

months = {'Jan': 1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}

class Tweet:
	def __init__(self, tweet):
		try:
			self.id_str = tweet['id_str']
			self.contents = tweet['text'].lower()
			#This is a list of hashtags format [{"indices":[x,y], "text": "blah"}, ... ]
			self.hashtags = []
			self.keywords = list(set(self.contents.split()))
			for tag in tweet['entities']['hashtags']:
				self.hashtags.append(tag['text'])
			#This will be a date object
			tokens = tweet['created_at'].split(' ')
			self.date = datetime(int(tokens[5]), months[tokens[1]], int(tokens[2]))
			if tweet['coordinates']:
				self.location = {'type': tweet['coordinates']['type'], 'lat': tweet['coordinates']['coordinates'][1], 'lng': tweet['coordinates']['coordinates'][0]}
				if self.location['lng'] == 0 and self.location['lat'] == 0:
					self.valid = False
				else:
					self.valid = True
					#print "Tweet: " + self.id_str + " loaded."
			else:
				self.valid = False
		except KeyError as e:
			self.valid = False
			print "Missing data from tweet: " + str(e)
			print tweet.keys()
		
	def __str__(self):
		return "Location type: {0} at ({1}, {2})".format(self.location['type'], self.location['lat'], self.location['lat'])
	
	# This function returns a list of dictionaries of the same tweet on with
	# different keywords
	def toDictionary(self):
		temp_dict = {
			'id_str' : self.id_str,
			'contents' : self.contents,
			'hashtags' : self.hashtags,
			'date' : self.date,
			'location' : self.location,
			'valid' : self.valid,
			'keywords' : self.keywords
		}
			
		return temp_dict
		

def PopulateDB(tweets):
	connection = Connection()
	db = connection.GeoTaggedTweets
	DK_index = db.DateKeywordCollection
	H_index = db.HashtagCollection
	DK_index.create_index([('date', DESCENDING), ('keywords', ASCENDING)])
	H_index.create_index('hashtags')
		
	for tweet in tweets:
		DK_index.insert(tweet.toDictionary())
		H_index.insert(tweet.toDictionary())
		
def LoadTweets(filenames):
	for filename in filenames:
		tweets = []
		file = open(filename)
		# List of tweet objects
		for line in file:
			temp = Tweet(json.loads(line))
			if temp.valid:
				tweets.append(temp)
		file.close()
		PopulateDB(tweets)
	
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
	import sys
	LoadTweets(sys.argv[1:])