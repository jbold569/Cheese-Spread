# Hash Tag Spatial-Temporal Distribution Demo
#
# Author: Jason Bolden
# Date: June 05, 2012
#
#

from datetime import *
import json

months = {'Jan': 1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}

class Tweet:
	def __init__(self, tweet):
		try:
			self.contents = tweet['text']
			#This is a list of hashtags format [{"indices":[x,y], "text": "blah"}, ... ]
			self.hashtags = []
			for tag in tweet['entities']['hashtags']:
				self.hashtags.append(tag['text'])
			#This will be a date object
			tokens = tweet['created_at'].split(' ')
			self.date = date(int(tokens[5]), months[tokens[1]], int(tokens[2]))
			self.location = Location(coord = tweet['coordinates'])
			if self.location.lon == 0 and self.location.lat == 0:
				self.valid = False
			else:
				self.valid = True
		except KeyError:
			self.valid = False
			#print "Missing data from tweet"

	def __str__(self):
		pass		

class Location:
	def __init__(self,lon=0,lat=0, coord = None):
		if coord:
			self.type = coord['type']
			self.lon = coord['coordinates'][0]
			self.lat = coord['coordinates'][1]
		else:
			self.type = 'Point'
			self.lon = lon
			self.lat = lat
	
	def __str__(self):
		return "Location type: {0} at ({1}, {2})".format(self.type, self.lon, self.lat)
		
def main(argv):
	query = raw_input("Enter your event keyword: ")
	input  = raw_input("Enter the date of the event(dd-mm-yyyy): ")
	tokens = input.split('-')
	event_date = date(int(tokens[2]), int(tokens[1]), int(tokens[0]))
	
	desired_tweets = FilterByQuery(query, event_date, LoadTweets(argv[1:]))
	
	# dictionary form {"HashTag": (df,"HashTag"), ...}
	hashtag_dfs = CountHashtags(desired_tweets)
	
	print hashtag_dfs
	# mapping function

# Returns a list of all tweets that meet the query criteria
def FilterByQuery(query, date, tweets):
	filtered_tweets = []
	
	for tweet in tweets:
		if not tweet.valid or tweet.date < date:
			continue
		if query.lower() in tweet.contents.lower():
			filtered_tweets.append(tweet)
			#print tweet.contents
			#print tweet.hashtags
			print tweet.location

	#print len(filtered_tweets)
	return filtered_tweets
	
def LoadTweets(filenames):
	tweets = []
	for filename in filenames:
		file = open(filename)
		# List of tweet objects
		for line in file:
			tweets.append(Tweet(json.loads(line)))
	return tweets

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
	
if __name__ == "__main__":
	import sys
	main(sys.argv)
