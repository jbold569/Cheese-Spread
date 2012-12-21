from DataObjects import *
import DatabaseInterface as dbi
import datetime as dt
import utils, json, gzip, os, sys

class DataLoader():
	def __init__(self, index=False, path='/mnt/chevron/jason/tweets/test'):
		self.filenames = []
		for path, names, files in os.walk(path):
			for file in files:
				self.filenames.append(os.path.join(path, file))
		self.DBI = dbi.DatabaseInterface()
		
		# Used to handle entropy
		self.keywordStats = []
		
		if index:
			self.DBI.index()
			
	def loadTweets(self):
		# This will be used for entropy
		keywordStats = []
		
		for filename in self.filenames:
			time_period = utils.parseTimePeriod(filename)
			
			file = gzip.open(filename, 'r')
			print "Loading: " + filename
			
			# Time Period Stats
			tpsObj = TimePeriodStat(time_period, bound=utils.USA)
			
			# Dictionary of Keyword Stats
			# Structure { keyword: keywordObj, ...}
			dKeywordStats = {}
			
			for line in file:
				try:
					tweetObj = Tweet(json.loads(line))
					# Check if tweet is valid
					if tweetObj.valid and tweetObj.bound == utils.USA:
						self.DBI.updateTweets(tweetObj)
						tpsObj.incTweets()
					else:
						continue
						
					# Update Keyword Stats
					for word, term_freq in tweetObj.dTermFreqs.iteritems():
						try:
							dKeywordStats[word].incFreqs(term_freq)
						except KeyError:
							dKeywordStats[word] = KeywordStat(word, time_period, bound=utils.USA)
							dKeywordStats[word].incFreqs(term_freq)
							tpsObj.incKeywords()
					
					for tag in tweetObj.hashtags:
						try:
							dKeywordStats[word].incFreqs()
						except KeyError:
							dKeywordStats[word] = KeywordStat(tag, time_period, bound=utils.USA, poh=1)
							dKeywordStats[word].incFreqs()
							tpsObj.incHashtags()
				except:
					print "Bad JSON"
			# Update Statistics
			for statObj in dKeywordStats.values():
				self.DBI.updateKeywordStats(statObj)
			
			self.DBI.updateTimePeriodStats(tpsObj)
			
			# Handle Entropy
			self.keywordStats.append((time_period, dKeywordStats))
			if len(self.keywordStats) == 7:
				self.updateEntropy()
				
			# Close the file of tweets
			file.close()
			
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
			self.DBI.updateKeywordStats({"$and":[{'date': e_date}, {'keyword': keyword}]}, {'$set': {'entropy': tfs}})
		self.keywordStats.pop(0)
