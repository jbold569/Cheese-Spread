# Hash Tag Spatial-Temporal Distribution Demo
#
# Author: Jason Bolden
# Date: June 05, 2012
#
#

from pymongo import *
from datetime import *
import EventTracker_AF as et
import json
import cherrypy


date_counter = 0
event_counter = 0
class Server(object):
	results = []
	
	@cherrypy.expose
	def QueryEvents(self, startDate, endDate):
		global date_counter, event_counter
		start  = startDate
		end = endDate
		tokens = start.split('-')
		event_start_date = datetime(int(tokens[2]), int(tokens[1]), int(tokens[0]))
		tokens = end.split('-')
		event_end_date = datetime(int(tokens[2]), int(tokens[1]), int(tokens[0]), 23, 59, 59)
		# Mongo sequence
		print event_start_date
		print event_end_date
		
		connection = Connection(host = 'hamm.cse.tamu.edu')
		db = connection.GeoTaggedTweets
		DK_index = db.DateKeywordCollection
		
		# Format {date: [tweet1, tweet2, ...], ...} 
		desired_tweets = {}
		temp = event_start_date
		results = DK_index.find({'$and':[{'date' : {'$gte': event_start_date}}, {'date' : {'$lte': event_end_date}}, {"bound":"USA"}]}, limit=10)
		print "Results returned: ",
		#while results:
		print results.count(with_limit_and_skip=True)
		#query_date = None
				#query_date = tweet['date']
			#print query_date
			#results = DK_index.find({'$and':[{'date' : {'$gt': query_date}}, {'date' : {'$lte': event_end_date}}]}, limit=5000)
				
		#print desired_tweets
		# dictionary form {"HashTag": (df,"HashTag"), ...}
		#hashtag_dfs = CountHashtags(desired_tweets)
		# Structure will be a 2D array, first dimension is divided by day and the 
		# second is divided by event
		# Structure {date: [event, ...], ...}
		events = et.FindEvents(results, db)
		
		data = {}
		data['events'] = []
		for i in range(5*event_counter,5*event_counter+5):
			data['events'].append(events.values()[date_counter][i].toDictionary())
		# The server only needs 5 events at a time, no use in sending the whole data structure
		# inputs from the client
		# 1) page of events to view
		# 2) date within the query span
		
		#print data
		obj = json.dumps( data, allow_nan=False, default=date_handler)
		print obj
		return obj
		
def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj
	
cherrypy.quickstart(Server(), config="etc/web.conf")

