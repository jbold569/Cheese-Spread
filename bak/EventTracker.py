import json
import datetime as dt
import numpy as np
		
EVENT_ID = 0
DIMENSION = 100
ORIGIN = np.ones(DIMENSION)/DIMENSION**0.5
database = None
alpha, beta, gamma = 0.1, 0.4, 0.5 # tf-idf, poh, entro

class Event:
	def __init__(self, id, centroid):
		self.centroid = centroid
		self.id = id
		self.score = 0
		# Standard Deviation
		self.std_dev = 0
		# ID vector pairs (id, vec)
		self.tweet_stats = []
		# Tweet object vector pairs (object, vec, simularity)
		self.tweets = []
		#self.parent = parent_id
	
	def __str__(self):
		string = "Event ID: %i\nNumber of tweets: %i" %(self.id, len(self.tweets))
		string += "\nCentroid: \n\t" + str([ '%.6f' % elem for elem in self.centroid])
		string += "\nStandard Deviation: \n\t" + str([ '%.6f' % elem for elem in self.std_dev])
		string +="\nTweets: \n"
		for i in range(len(self.tweets)):
			string += str(i+1) + ") \n\t" + self.tweets[i][0]['contents'].encode('utf-8')
			string += "\n\t" + str([ '%.6f' % elem for elem in self.tweets[i][1]])
			# Prints the distance vector of the tweet in std_dev
			string += "\n\tSimularity: " + str(self.tweets[i][2]) + "\n"
		return string
		
	def addTweet(self, vec):
		stat = (vec[0], vec[1], sim(self.centroid, vec[1]))
		self.tweet_stats.append(stat) 
	
	def clearTweets(self):
		self.tweet_stats = []
	
	# This function takes the list of all tweets for the day and adds the ones
	# in this event's cluster to the tweets list. This list will then besorted and
	# used to print out the contents of the tweets in this event
	def loadTweets(self, tweets):
		for id, vec, simu in self.tweet_stats:
			self.tweets.append((tweets[id], vec, simu))
			
		cmp_tweets = lambda vec: sim(vec[1], self.centroid)
		self.tweets = sorted(self.tweets, key = cmp_tweets, reverse=True)
		
	# Calculates the new centroid from nearest tweets
	def calculateCentroid(self):
		global DIMENSION
		centroid = np.zeros(DIMENSION)
		#print vects
		for id,vect,simu in self.tweet_stats:
			centroid += np.array(vect)
		centroid /= np.linalg.norm(centroid)
		simularity = sim(self.centroid, centroid)
		self.centroid = centroid
		total = np.zeros(DIMENSION)
		for id,vect,simu in self.tweet_stats:
			total += (np.array(vect)- self.centroid)**2
		if len(self.tweet_stats) == 0:
			self.std_dev = total
		else:
			self.std_dev = (total/len(self.tweet_stats))**0.5	
		return simularity
		
# This the cosine simularity function
def sim(doc1, doc2):
	global DIMENSION
	score = 0.0
	for i in range(DIMENSION):
		score += doc1[i]*doc2[i]
	return score

def genSeedCentroids(k):
	global DIMENSION
	from random import random
	seeds = []
	for i in range(k):
		seed = np.zeros(DIMENSION)
		for j in range(DIMENSION):
			val = random()*100
			seed[j] = val
					
		# Determine the magnitude of the vector
		mag = np.linalg.norm(seed)
					
		# Normalize the vector
		if mag == 0:
			continue
		seed /= mag
		seeds.append(Event(i,seed))
	return seeds
	
# Structure {tweet.id: [normalized vector], ...}
def cluster(vecs, k):
	global EVENT_ID
	# Structure [ Event, ...]
	clusters = []
	
	# list of events
	events = genSeedCentroids(k)
	#for event in events:
	#	print event.centroid
	#	x = raw_input("Next")
	running = True
	
	while running:
		# Clear the events of tweets from previous iteration
		for i in range(k):
			events[i].clearTweets()
		running = False
		# assign tweets to their closest centroids
		for id,vec in vecs.iteritems():
			# cosine of 90 = 0
			min = (-1,0)
			for i in range(len(events)):
				dist = sim(events[i].centroid, vec)
				if dist >= min[1]:
					min = (i, dist)
			#print "Adding a tweet to event: " + str(min[0])
			events[min[0]].addTweet((id,vec))
		print "\n"
		for event in events:
			print "Event: " +  str(event.id) 
			print "tweets: " + str(len(event.tweet_stats))
			simularity = event.calculateCentroid()	 
			print "simularity: " + str(simularity)
			print ""
			if simularity < .98:
				running = True
			
	for i in range(k):
		events[i].id = EVENT_ID
		EVENT_ID += 1
	return events
			
def getStats(word, date, totals):
	global database
	occurrences, = database['OccurrencesCollection'].find({'$and': [{"date": date}, {"keyword": word}, {"bound":"USA"}]})
	from math import log10
	df = float(occurrences['df'])/totals['total_tweets']
	tf = occurrences['tf']
	tf_idf = tf/totals['total_keywords']*log10(totals['total_tweets']/df)
	poh = float(occurrences['poh'])
	tfs = occurrences['entropy']
	
	# If there isn't enough information to calculate the entropy 
	if len(tfs) < 7:
		return tf_idf, poh, 0
	else:
		entro = 0.0
		total_t = 0.0
		total_d = 0.0
	
		for tf in tfs:
			total_t += tf
		for tf in tfs:
			prob_i = tf/total_t
			entro += prob_i*log10(prob_i+1)
		
		return tf_idf, poh, entro
			
def getTopicalWords(date_tweet_pairs):
	global database, alpha, beta, gamma
	
	# Structure {date: {word1: quality , word2: quality , ...}, ...}
	term_qualities = {}
	print "Calculating metrics..."
	for date in date_tweet_pairs.keys():
		print "Calculating metrics for: ",
		print date
		term_qualities[date] = []
		keywords_set = []
		print "Creating keyword set..."
		results = database['OccurrencesCollection'].find({"$and":[{'date': date}, {"bound":"USA"}]})
		for result in results:
			keywords_set.append(result['keyword'])
		print "Done creating set.\n"
		
		total_entropy = 0.0
		totals, = database['KeywordCountCollection'].find({"$and":[{"date": date}, {"bound":"USA"}]})
		for word in keywords_set:
			idf,poh,entro = getStats(word, date, totals)
			term_qualities[date].append((word, alpha*idf+beta*poh+gamma*entro)) 
		
	print "Done calculating metrics.\n"
	file = open("Stats", 'w')
	temp = {}
	for key, value in term_qualities.items():
		temp[str(key)] = value
	file.write(json.dumps(temp, sort_keys=True, indent=4))
	file.close()
	
	# Structure {date: [word1, word2, ...], ...}
	topicalwords = {}
	for date, word_quality_pairs in term_qualities.items():
		topicalwords[date] =sorted(word_quality_pairs, key=lambda pair: pair[1],  reverse=True)[:DIMENSION]
	
	return topicalwords		

def getDocVectors(topicalwords, tweets):
	global DIMENSION
	# Dictionary that represents the tweets in the topicalword
	# vector space
	# Structure {date: { tweet.id: [normalized vector], ...}, ...}
	tweet_vectors = {}
	for date, tWords in topicalwords.items():
		tweet_vectors[date] = {}
		#print date
		for id, tweet in tweets[date].iteritems():
			#print id
			#print tweet
			vec = np.zeros(DIMENSION)
			total = 0.0
			for i in range(DIMENSION):
				val = tweet['contents'].split().count(tWords[i][0])
				if val == 0:
					val = tweet['hashtags'].count(tWords[i][0])
				total += val**2
				vec[i] = val
				
			# Determine the magnitude of the vector
			mag = np.linalg.norm(vec)
				
			# Normalize the vector
			if mag == 0:
				tweet_vectors[date][id] = list(vec)
			else:
				tweet_vectors[date][id] = list(vec/mag)
	return tweet_vectors
	
def FindEvents(tweets, db):
	global database
	
	# Structure of ordered_tweets {date: {_id:tweet1,..}, ...} 
	ordered_tweets = {}
	for tweet in tweets:		
			day = dt.datetime(tweet['date'].year, tweet['date'].month, tweet['date'].day)
			try:
				ordered_tweets[day][tweet['_id']] = tweet
			except KeyError:
				ordered_tweets[day] = {}
				ordered_tweets[day][tweet['_id']] = tweet
	database  = db
		
	# Structure {date: [word1, word2, ...], ...}
	topicalwords = getTopicalWords(ordered_tweets)
	# output the topicalwords to a file
	file = open("Topicalwords", 'w')
	temp = {}
	for key, value in topicalwords.items():
		temp[str(key)] = value
	file.write(json.dumps(temp, sort_keys=True, indent=4))
	file.close()
	
	# Structure {date: { tweet.id: [normalized vector], ...}, ...}
	doc_vectors = getDocVectors(topicalwords, ordered_tweets)
	file = open("Vectors", 'w')
	temp = {}
	for key, value in doc_vectors.items():
		temp[str(key)] = value
	file.write(json.dumps(temp, sort_keys=True, indent=4))
	file.close()
	
	# Structure  {date: [Events, ...], ...}
	events = {}
	file = open("Events", 'w')
	# iterate through the dictionary and cluster for every day
	for date, vectors in doc_vectors.items():
		events[date] = cluster(vectors, 50)
		file.write(str(date) + ": \n")
		for event in events[date]:
			event.loadTweets(ordered_tweets[date])
			file.write(str(event) + "\n\n")
	file.close()
	