# Hash Tag Spatial-Temporal Distribution Demo
#
# Author: Jason Bolden
# Date: June 05, 2012
#
#

from pymongo import *
from datetime import *
import json
import cherrypy

argv = []

# Deprecated due ot Mongo
# Returns a list of all tweets that meet the query criteria
def FilterByQuery(query, date):
	filtered_tweets = []
	
	for tweet in tweets:
		if not tweet.valid or tweet.date < date:
			continue
		if query.lower() in tweet.contents.lower():
			filtered_tweets.append(tweet)
			#print tweet.contents
			#print tweet.hashtags
			#print tweet.location

	#print len(filtered_tweets)
	#return filtered_tweets
	


class Server(object):
	@cherrypy.expose
	def query(self, keyword, startDate, endDate):
		print keyword
		print startDate
		print endDate
		
		start  = startDate
		end = endDate
		tokens = start.split('-')
		event_start_date = datetime(int(tokens[2]), int(tokens[1]), int(tokens[0]))
		tokens = end.split('-')
		event_end_date = datetime(int(tokens[2]), int(tokens[1]), int(tokens[0]))
		# Mongo sequence
		connection = Connection(host = 'hamm.cse.tamu.edu')
		db = connection.GeoTaggedTweets
		DK_index = db.DateKeywordCollection
		
		
		desired_tweets = []
		results = DK_index.find({'keywords': keyword.lower(), 'date' : {'$gte': event_start_date}, 'date' : {'$lte': event_end_date}}, limit = 500)
		print results.count()
		for tweet in results:
			#print tweet
			desired_tweets.append(tweet)
		#print desired_tweets
		# dictionary form {"HashTag": (df,"HashTag"), ...}
		#hashtag_dfs = CountHashtags(desired_tweets)
		hashtag_info = {}
		
		for tweet in desired_tweets:
			#print tweet
			for hashtag in tweet['hashtags']:
				#print hashtag
				if hashtag not in hashtag_info.keys():
					hashtag_info[hashtag] = []
				temp = {}
				temp['hashtag'] = hashtag
				temp['loc'] = tweet['location']['lat'], tweet['location']['lng']
				temp['date'] = str(tweet['date'].day)+'-'+str(tweet['date'].month)+'-'+str(tweet['date'].year)
				hashtag_info[hashtag].append(temp)
		#print hashtag_info		
		data = {}
		data['tags'] = []
		tags = hashtag_info.keys()
		sorted_tags = sorted(tags, key = lambda hashtag: len(hashtag_info[hashtag]), reverse=True)
		for tag in sorted_tags:
			data['tags'].append(hashtag_info[tag])
		return json.dumps(data, separators=(',',':'))
		
cherrypy.quickstart(Server(), config="etc/web.conf")

