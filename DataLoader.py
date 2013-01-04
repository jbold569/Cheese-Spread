from DataObjects import *
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
		# Used to handle entropy
		self.keywordStats = []
		self.initial_time_period = None
		if index:
			self.DBI.index()
			
	def loadTweets(self):
		# This will be used for entropy
		keywordStats = []
		
		# These variables are persistant through different files due to overlap
		# Time Period Stats Object
		tpsObj = None
		# Maximum size of 1000 tweets
		tweets = []	
		
		time_period = None
		
		for filename in self.filenames:
			print "Loading: " + filename
			file = gzip.open(filename, 'r')
			
			# Dictionary of Keyword Stats
			# Structure { keyword: keywordObj, ...}
			dKeywordStats = {}
			out = -1
			last_date = None
			for line in file:
				tweetObj = None
				probe.StartTiming("LoadedTweets")
				try:
					tweetObj = Tweet(json.loads(line))			
					if not self.initial_time_period:
						self.initial_time_period = utils.determineTimePeriod(tweetObj.date)
						time_period = self.initial_time_period
						print "\nStarting time period at: ",
						print self.initial_time_period
						
					if out == -1:
						out+=1
						print "First tweet: ",
						print tweetObj.date
				except ValueError as e:
					print e
					continue
				probe.StopTiming("LoadedTweets")
					
				if not utils.inTimePeriod(time_period, tweetObj.date):
					time_period = time_period + dt.timedelta(minutes=15)									
					print "\nChanging time period to: ",
					print time_period
					# Update Time Period Stats
					self.DBI.updateDatabase(tpsObj, "TimePeriodStatsCollection")
					tpsObj = TimePeriodStat(time_period, bound=utils.USA)

				# Check if tweet is valid
				if tweetObj.valid and tweetObj.bound == utils.USA:
					tweets.append(tweetObj)
					if len(tweets) == 1000:
						self.DBI.insertToDatabase(tweets, "TweetsCollection")
						tweets = []
					tpsObj.incTweets()
					last_date = tweetObj.date
				else:
					#print "Invalid Tweet"
					continue
					
				# Update Keyword Stats
				for word, term_freq in tweetObj.dTermFreqs.iteritems():
					poh = 0
					if word in tweetObj.hashtags:
						poh = 1
						tpsObj.incHashtags()
					try:
						dKeywordStats[word].incFreqs(term_freq)
					except KeyError:
						dKeywordStats[word] = KeywordStat(word, time_period, bound=utils.USA, poh = poh)
						dKeywordStats[word].incFreqs(term_freq)
						tpsObj.incKeywords()
			
			print "Date of last tweet in file: ",
			print last_date
			print "Total keywords parsed: " + str(len(dKeywordStats))
			
			# Update Keyword Statistics
			self.DBI.insertToDatabase(dKeywordStats.values(), "KeywordStatsCollection")
			
			# Handle Entropy
			self.keywordStats.append((time_period, dKeywordStats))
			#if len(self.keywordStats) == 7:
			#	print "Calculating Entropy"
			#	self.updateEntropy()
				
			# Close the file of tweets
			file.close()
	
	def LoadStats():
		'''if not utils.inTimePeriod(time_period, tweetObj.date):
			time_period = time_period + dt.timedelta(minutes=15)									
			print "\nChanging time period to: ",
			print time_period
			# Update Time Period Stats
			self.DBI.updateDatabase(tpsObj, "TimePeriodStatsCollection")
			tpsObj = TimePeriodStat(time_period, bound=utils.USA)
		'''
		pass
		
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
