from DataObjects import *
import utils
import pymongo as mdb

#file format: geo.2012-01-01_08-51.txt.gz
class DatabaseInterface():
	def __init__(self, host='localhost'):
		self.db = mdb.Connection(host=host).GeoTaggedTweets
	
	def queryKeywordStats(self, time_period=None, keyword='', bound=utils.USA, query=None):
		if not query: query = {'$and': [{"time_period": time_period}, {"keyword": keyword}, {"bound":bound}]}
		return self.db['KeywordStatsCollection'].find(query)
	
	def queryTweets(self, start_time=None, end_time=None, bound=utils.USA, query=None):
		if not query: query = {'$and':[{'date' : {'$gte': start_date}}, {'date' : {'$lte': event_end_date}}, {"bound":bound}]}
		return self.db['TweetsCollection'].find(query)
	
	def queryTimePeriodStats(self, time_period=None, bound=utils.USA, query=None):
		if not query: query = {'$and': [{"time_period": time_period}, {"bound":bound}]}
		return self.db['TimePeriodStatsCollection'].find(query)
	
	#expects a KeywordStat Object
	def updateKeywordStats(self, data):
		if type(data) == type(KeywordStat):
			data = data.toDBObject()
		self.db['KeywordStatsCollection'].update(data[0], data[1], upsert=True)
	
	#expects a Tweet Object
	def updateTweets(self, data):
		if type(data) == type(Tweet):
			data = data.toDBObject()
		self.db['TweetsCollection'].update(data[0], data[1], upsert=True)
	
	#expects a TimePeriodStat Object	
	def updateTimePeriodStats(self, data):
		if type(data) == type(TimePeriodStat):
			data = data.toDBObject()
			print "Entered"
		self.db['TimePeriodStatsCollection'].update(data[0], data[1], upsert=True)
		
	def index(self):
		self.db['KeywordStatsCollection'].ensure_index([('date',1), ('keywords',1), ("bound",1)], background=True)
		self.db['TweetsCollection'].ensure_index([('date',1), ('keyword',1), ("bound",1)], background=True)
		self.db['TimePeriodStatsCollection'].ensure_index([('date',1), ("bound",1)], background=True)
		
