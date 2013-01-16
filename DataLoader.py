from DataObjects import *
from guppy import hpy
import DatabaseInterface as dbi
import datetime as dt
import utils, json, gzip, os, sys, Probe

probe = Probe.Probe()

class DataLoader():
	def __init__(self, index=False, path='/mnt/chevron/bde/Data/TweetData/GeoTweets/2012/5'):
		self.filenames = []
		for path, names, files in os.walk(path):
			for file in files:
				self.filenames.append(os.path.join(path, file))
		self.DBI = dbi.DatabaseInterface()
		self.filenames.sort(key=lambda filename:utils.parseTimePeriod(filename)) 
		# Entropy
		self.keywordStats = []
		self.initial_time_period = None
		self.h = hpy()
		if index:
			self.DBI.index()
			
	def loadTweets(self):
		# These variables are persistant through different files due to overlap
		
		# list of time period stat objects
		tpsIndex = []
		# Structure {date: tpsObj, ...}
		dTPSObjs = {}
		
		# Maximum size of 1000 tweets
		tweets = []	
		
		time_period = None
		
		for filename in self.filenames:
			print "Loading: " + filename
			file = gzip.open(filename, 'r')
			
			# Dictionary of Keyword Stats
			# Structure { keyword: keywordObj, ...}
			dKeywordStats = {}
			for line in file:
				unique_periods = []
				tweetObj = None
				probe.StartTiming("LoadedTweets")
				try:
					tweetObj = Tweet(json.loads(line))
					if not tweetObj.valid:
						continue			
					
					# ************************************
					# New code
					# ********************************************
					
					
					if not self.initial_time_period:
						self.initial_time_period = tweetObj.time_period
						tpsIndex.append(self.initial_time_period)
						dTPSObjs[self.initial_time_period] = TimePeriodStat(self.initial_time_period, bound=utils.USA)
						print "\nStarting time period at: ",
						print self.initial_time_period
				except ValueError as e:
					print e
					continue
				probe.StopTiming("LoadedTweets")
					
				#if not utils.inTimePeriod(time_period, tweetObj.date):
				#	time_period += dt.timedelta(minutes=15)									
				#	print "\nChanging time period to: ",
				#	print time_period
					# Update Time Period Stats
				#	self.DBI.updateDatabase(tpsObj, "TimePeriodStatsCollection")
				#	tpsObj = TimePeriodStat(time_period, bound=utils.USA)

				# Check if tweet in bound
				if tweetObj.bound == utils.USA:
					tweets.append(tweetObj)
					if len(tweets) == 1000:
						self.DBI.insertToDatabase(tweets, "TweetsCollection")
						tweets = []
					try:	
						dTPSObj[tweetObj.time_period].incTweetStats(tweetObj)
					except IndexError:
						dTPSObjs[tweetObj.time_period] = TimePeriodStat(tweetObj.time_period, bound=tweetObj.bound)
						tpsIndex.append(tweetObj.time_period)
						if len(tpsIndex) == 3:
							self.DBI.updateDatabase(dTPSObjs[tpsIndex[0]], "TimePeriodStatsCollection")
							del dTPSObjs[tweetObj.time_period]
							tpsIndex.pop(0)
							
						dTPSObjs[tweetObj.time_period].incTweetStats(tweetObj)
						
					if not tweetObj.time_period in unique_periods:
						unique_periods.append(tweetObj.time_period)
				else:
					#print "Tweet out of bounds"
					continue
					
				# Update Keyword Stats [ Fix entropy again ]
				for word, term_freq in tweetObj.dTermFreqs.iteritems():
					poh = 0
					if word in tweetObj.hashtags:
						poh = 1
					try:
						dKeywordStats[word].incFreqs(term_freq)
					except KeyError:
						dKeywordStats[word] = KeywordStat(word, tweetObj.time_period, bound=tweetObj.bound, poh = poh)
						dKeywordStats[word].incFreqs(term_freq)
				
			# Update Keyword Statistics
			self.DBI.insertToDatabase(dKeywordStats.values(), "KeywordStatsCollection")
			print "Date of last tweet in file: ",
			print unique_periods
			print "Total keywords parsed: " + str(len(dKeywordStats))
					
			# Handle Entropy
			self.keywordStats.append((time_period, dKeywordStats))
			if len(self.keywordStats) == 7:
				print "Calculating Entropy"
				self.updateEntropy()
				
			# Close the file of tweets
			file.close()
			print self.h.heap()
		
	def updateEntropy(self):
		# middle time period
		e_date = self.keywordStats[3][0]
		for keyword, statObj in self.keywordStats[3][1].iteritems():
			tfs = []
			for data in self.keywordStats:
				try:
					tfs.append(data[1][keyword].term_freq)
				except KeyError:
					tfs.append(0)
			self.DBI.updateDatabase(({"$and":[{'time_period': e_date}, {'keyword': keyword}, {'bound': utils.USA}]}, {'$set': {'entropy': tfs}}), "KeywordStatsCollection")		
		self.keywordStats.pop(0)

def main():
	probe.InitProbe("LoadedTweets", "%.3f tweets loaded a second.", 10)
	probe.InitProbe("LoadedKeywordStats", "%.3f Keyword Statistics loaded a second.", 10)
	probe.InitProbe("DictionaryConversion", "%.3f Keyword Statistics Converted a second.", 5)
	
	probe.RunProbes()
	loader = DataLoader(index=True)
	loader.loadTweets()

if __name__ == '__main__':
	main()
