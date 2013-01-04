# Hash Tag Spatial-Temporal Distribution Demo
#
# Author: Jason Bolden
# Date: June 05, 2012
#
#

import datetime as dt
import EventTracker_AF as et
import DatabaseInterface as dbi
import cherrypy, jason


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
		event_start_date = datetime(2012, 3, 28, 9, 18)
		tokens = end.split('-')
		event_end_date = datetime(2012, 3, 28, 9, 18+15)
		# Mongo sequence
		print event_start_date
		print event_end_date
		
		DBI = dbi.DatabaseInterface(host='hamm.cse.tamu.edu')
		
		#connection = Connection(host = 'hamm.cse.tamu.edu')
		#db = connection.GeoTaggedTweets
		#T_index = db.TweetsCollection
		#print T_index.find_one()
		# Format {date: [tweet1, tweet2, ...], ...} 
		desired_tweets = {}
		#start_time=None, end_time=None, bound=utils.USA, query=None
		# NOTE: query by time periods in the time span and stucture accordingly
		results = DBI.queryTweets(start_time=event_start_date, end_time = event_end_date)
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
		events = et.FindEvents(results)
		
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

