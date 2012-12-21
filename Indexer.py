# Tweet Indexer
#
# Author: Jason Bolden
# Date: June 15, 2012
#
#

from pymongo import *
from datetime import *
import Probe
import json
import re, string
import gzip

DEBUGGING = False

tweets_total = 0
tweets_inbound = 0
tweets_outbound = 0
tweets_invalid = 0
tweets_valid = 0

probe = Probe.Probe()
REINDEX = False
months = {'Jan': 1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}

def WordFilter(words):
	
	punc = re.compile('[%s]'%re.escape(string.punctuation))
	num = re.compile('[%s]'%re.escape(string.digits))
	alpha = re.compile('[^a-z]')
	white = re.compile('[\s]')
	keywords = []
	file = open("stopwords//lextek.txt", 'r')
	stopwords = file.read().split()
	file.close()
	
	for word in words:
		probe.StartTiming("wordFilter")
		# ignore long strings
		if len(word) > 20:
			continue

		temp_word = punc.sub('',word)
		temp_word = num.sub('',temp_word)

		# ignore unicode
		if re.search(alpha, word) != None:
			continue
		
		# ignore url
		if u'http' in word:
			continue
	
		# ignore mentions
		if word[0] == '@':
			continue
		
		# ignore stopwords
		try:
			if temp_word in stopwords:
				continue
		except UnicodeWarning:
			print temp_word
		
		# ignore empty string
		if len(temp_word) == 0:
			continue
		if '\x00' in temp_word:
			temp_word = string.replace(temp_word, '\x00', '')
			print temp_word
			
		keywords.append(temp_word)
		probe.StopTiming("wordFilter")
		
	return keywords
		
class Tweet:
	def __init__(self, tweet):
		global DEBUGGING, hashtags, tweets_invalid, tweets_valid
		try:
			# Information about the tweet
			self.id = tweet['id']
			self.retweet_count = tweet['retweet_count']
			self.contents = tweet['text'].lower()
			keywords = WordFilter(self.contents.split())
			self.keywords = list(set(keywords))
			self.dTermFreqs = {}
			for word in self.keywords:
				self.dTermFreqs[word] = keywords.count(word)
				
			self.urls = []
			self.user_mentions = []
			self.hashtags = []
			
			for url in tweet['entities']['urls']:
				self.urls.append(url['expanded_url'])
			for mention in tweet['entities']['user_mentions']:
				self.user_mentions.append(mention['id'])
			for tag in tweet['entities']['hashtags']:
				self.hashtags.append(tag['text'])
				try:
					a = hashtags[tag['text']]
				except KeyError:
					hashtags[tag['text']] = True

			#This will be a date object
			tokens = tweet['created_at'].split(' ')
			time = tokens[3].split(':')
			self.date = datetime(int(tokens[5]), months[tokens[1]], int(tokens[2]), int(time[0]), int(time[1]), int(time[2]))
			self.valid = True
			
			if tweet['coordinates']:
				if DEBUGGING:
					print "Exact location."
				self.location = {'type': tweet['coordinates']['type'], 'shape': None, 'lat': tweet['coordinates']['coordinates'][1], 'lng': tweet['coordinates']['coordinates'][0]}
				tweets_valid += 1
			elif tweet['place']:
				if DEBUGGING:
					print "Bounding Box"
				shape = []
				for coord in tweet['place']['bounding_box']['coordinates'][0]:
					shape.append({'lat': coord[1], 'lng': coord[0]})
				self.location = {'type': tweet['place']['bounding_box']['type'], 'shape': shape, 'lat': None, 'lng': None }
				tweets_valid += 1
			else:
				tweets_invalid += 1
				self.valid = False
				
				
			# Information about the user
			self.user = tweet['user']['id']
			self.follower_count = tweet['user']['followers_count']
			
				
			
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
			'dTermFreqs': self.dTermFreqs,
			'urls': self.urls,
			'retweet_count': self.retweet_count,
			'user_mentions': self.user_mentions,
			'user': self.user,
			'follower_count': self.follower_count,
			'bound': "USA"
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
		DK_index.ensure_index([('date',1), ('keywords',1), ("bound",1)], background=True)
		#DK_index.reindex()
		H_index.ensure_index('hashtags', background=True)
		#H_index.reindex()
		O_index.ensure_index([('date',1), ('keyword',1), ("bound",1)], background=True)
		#O_index.reindex()	
		C_index.ensure_index('date', background=True)
		#C_index.reindex()

# Structure {word: stat, ...}

keyword_occurrences = {}
document_freqs = {}
hashtag_occurrences = {}
hashtags = {}
total_keywords = 0
total_hashtags = 0

term_freqs = []
def PopulateDB(tweet):
	global a,DK_index, H_index, document_freqs, keyword_occurrences, hashtag_occurrences, total_keywords, total_hashtags
	
	probe.StartTiming("dbInserts")
	DK_index.update({'_id': tweet.id}, tweet.toDictionary(), upsert=True)
	#H_index.update({'_id': tweet.id}, tweet.toDictionary(), upsert=True)
	probe.StopTiming("dbInserts")
	
	for word, count in tweet.dTermFreqs.items():
		try:
			keyword_occurrences[word] += count
			document_freqs[word] += 1
		except KeyError:
			keyword_occurrences[word] = count
			document_freqs[word] = 1	
			total_keywords += 1
			if total_keywords%1000 == 0:
				print "Total Keyword Count: " + str(total_keywords/1000.0) + "K."
	
	for tag in tweet.hashtags:
		try:
			keyword_occurrences[tag.lower()] += 1
			document_freqs[tag.lower()] += 1
                except KeyError:
			keyword_occurrences[tag.lower()] = 1
			document_freqs[tag.lower()] = 1
			hashtag_occurrences[tag.lower()] = 1

	total_hashtags += len(tweet.hashtags)

def inBounds(tweet):
	global tweets_inbound, tweets_outbound
	if not tweet.location['shape']:
		lat = tweet.location['lat']
		lng = tweet.location['lng']
		if lat <= 48 and lat >= 25 and lng >= -125 and lng <= -66:
			#print '('+str(lat) + ', '+str(lng) + ')' 
			tweets_inbound += 1
			return True
	
	tweets_outbound += 1
	return False
	
def LoadTweets(file_dict):
	InitDB()
	global hashtags, keyword_occurrences, document_freqs, hashtag_occurrences, total_keywords, total_hashtags, tweets_total, tweets_inbound, tweets_outbound, tweets_valid, tweets_invalid
	for month,days in file_dict.items():
		for day,filenames in days.items():
			document_freqs = {}
			keyword_occurrences = {}
	                hashtag_occurrences = {}
        	        total_keywords = 0.0
        	        total_hashtags = 0.0
			file_counter = 0
			tweet_counter = 0.0
			tweets_total = 0
			tweets_valid = 0
			tweets_invalid = 0
			tweets_inbound = 0
			tweets_outbound = 0
			Date = datetime(2012, int(month), int(day))
			
			for filename in filenames:
				file = gzip.open(filename, 'r')
				print "Loading: " + filename
				# line num, loaded tweets, duplicate tweets
				for line in file:
					try:
						temp = Tweet(json.loads(line))
						tweets_total += 1
						if temp.valid and inBounds(temp):
                                                	probe.StartTiming("parsedLine")
                                                	PopulateDB(temp)
                                                	probe.StopTiming("parsedLine")
							tweet_counter += 1						
					except ValueError:
						print "Bad JSON."
				file.close()
				print filename + " loaded and Tweet objects inserted."
				file_counter += 1
				print str(file_counter) + " out of " + str(len(filenames)) + " loaded."
			if total_keywords == 0:
				continue
			#file = open("keywords/" + str(Date).split()[0]+".txt", 'w')
			#file.write(str(total_keywords) + '\n')
			#file.write(json.dumps(keyword_occurrences, sort_keys=True, indent=4))
			#file.close()
			total_hashtags = len(hashtags.keys())
				
			for keyword,tf in keyword_occurrences.items():
				try:
					O_index.update({"$and":[{'date': Date}, {'keyword': keyword}]}, {'date': Date, 'keyword': keyword, 'tf': tf, 'df': document_freqs[keyword],\
					'poh': hashtag_occurrences[keyword], 'entropy': [], 'bound': "USA"}, upsert=True)
				except KeyError:
					O_index.update({"$and":[{'date': Date}, {'keyword': keyword}]}, {'date': Date, 'keyword': keyword, 'tf': tf, 'df': document_freqs[keyword],\
					'poh': 0, 'entropy': [], 'bound': "USA"}, upsert=True)
			
			# These two if statements handle entropy
			if len(term_freqs) < 7:
				term_freqs.append((Date, keyword_occurrences))
			
			if len(term_freqs) == 7:
				print term_freqs[3][0]
				e_date = datetime(term_freqs[3][0].year, term_freqs[3][0].month, term_freqs[3][0].day)
				for keyword in term_freqs[3][1].keys():
					tfs = []
					for data in term_freqs:
						try:
							tfs.append(data[1][keyword])
						except KeyError:
							tfs.append(0)
					O_index.update({"$and":[{'date': e_date}, {'keyword': keyword}]}, {'$set': {'entropy': tfs}}, upsert=True)
				term_freqs.pop(0)
				
			C_index.update({'date': Date}, {'date':Date, 'total_keywords': total_keywords, 'total_hashtags': total_hashtags, 'bound': "USA",\
					'total_tweets': tweet_counter},	upsert=True)
			file = open("Stats", 'w+')
			file.write(Date.isoformat())
			string = "\nTotal Tweets: %i\nValid: %i\nInvalid: %i\nInbound: %i\nOutbound: %i\n\n"%(tweets_total, tweets_valid, tweets_invalid, tweets_inbound, tweets_outbound)
			file.write(string)
			file.close()
if __name__ == '__main__':
	import os, sys
	if sys.argv[1] == 'True':
		DEBUGGING = True
	if sys.argv[2] == 'True':
		REINDEX = True
	
	probe.InitProbe("parsedLine", "%.3f tweets parsed a second.", 10)
	probe.InitProbe("dbInserts", "%.3f database inserts a second.", 10)
	probe.InitProbe("wordFilter", "%.3f keywords filtered a second.\n", 10)
	probe.RunProbes()
		
	filenames = []
	#Path to the tweets, on chevron make this adjustable later
	for path, names, files in os.walk('/mnt/chevron/jason/tweets/'+sys.argv[3]):
		for file in files:
			filenames.append(os.path.join(path, file))
	file_dict = {}
	for filename in filenames:
		# filename ../month/day/*.gz
		tokens = filename.split('/')
		month = int(tokens[-3])
		day = int(tokens[-2])
		try:
			if day not in file_dict[month].keys(): 
				file_dict[month][day] = []				
			file_dict[month][day].append(filename)
		except KeyError:
			file_dict[month] = {}
 			if day not in file_dict[month].keys():
                                file_dict[month][day] = []
                        file_dict[month][day].append(filename)
	LoadTweets(file_dict)
