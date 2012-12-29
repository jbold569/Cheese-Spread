from sklearn.cluster import affinity_propagation, k_means
import json
import datetime as dt
import numpy as np
		
EVENT_ID = 1000000
DIMENSION = 100
ORIGIN = np.ones(DIMENSION)/DIMENSION**0.5
database = None
alpha, beta, gamma = 0.5, 0.5, 0.0 # tf-idf, poh, entro

class Event:
	def __init__(self, id, centroid):
		self.centroid = centroid
		self.id = id
		self.quality = 0
		# Standard Deviation
		self.std_dev = 0
		# ID vector pairs (id, vec)
		self.tweet_stats = []
		# Tweet object vector pairs [object, vec, simularity]
		self.tweets = []
		self.parent = 0
	
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
	
	
	def toDictionary(self):
		simularities = []
		tweets = []
		for tweet, vec, sim in self.tweets:
			simularities.append(sim)
			tweets.append(tweet)
		temp_dict = {
			#'centroid': list(self.centroid[np.isnan(self.centroid)]="NaN"),
			'id': self.id,
			'quality': self.quality,
			#'std_dev': list(self.std_dev[np.isnan(self.std_dev)]="NaN"),
			'tweets': tweets,
			#'simularities': list(np.array(simularities)[np.isnan(np.array(simularities))]="NaN"),
			'parent_id': self.parent
		}
		return temp_dict
		
	def addTweet(self, vec):
		self.tweet_stats.append(vec) 
	
	def clearTweets(self):
		self.tweet_stats = []
	
	# This function takes the list of all tweets for the day and adds the ones
	# in this event's cluster to the tweets list. This list will then besorted and
	# used to print out the contents of the tweets in this event
	def loadTweets(self, tweets):
		for id, vec in self.tweet_stats:
			self.tweets.append([tweets[id], vec, sim(vec,self.centroid)])
			
		cmp_tweets = lambda vec: vec[2]
		self.tweets = sorted(self.tweets, key = cmp_tweets, reverse=True)
		
	# Calculates the new centroid from nearest tweets
	def calculateCentroid(self):
		global DIMENSION
		centroid = np.zeros(DIMENSION)
		#print vects
		for id,vect in self.tweet_stats:
			centroid += np.array(vect)
		centroid /= np.linalg.norm(centroid)
		self.centroid = centroid
		total = np.zeros(DIMENSION)
		for id,vect in self.tweet_stats:
			total += (np.array(vect)- self.centroid)**2
		if len(self.tweet_stats) == 0:
			self.std_dev = total
		else:
			self.std_dev = (total/len(self.tweet_stats))**0.5	
		
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
def cluster(vecs):
	global EVENT_ID
	s = np.zeros([len(vecs),len(vecs)])
	for D, i in zip(vecs.values(), range(len(vecs))):
		for d, j in zip(vecs.values(), range(len(vecs))):
			s[i][j] = -sim(D,d)
	centroids, labels, intertia = k_means(X=s, n_clusters=10)
	#cluster_centers, labels = affinity_propagation(S=s, preference=-0.0)
	cluster_tweet_pair = zip(vecs.keys(), labels)

	events = []
	print "Number of clusters: " + str(len(centroids))
	for i in range(len(centroids)):
		EVENT_ID += 1
		events.append(Event(EVENT_ID, centroids[i]))
		for id, label in cluster_tweet_pair:
			if label == i:
				events[i].addTweet((id,vecs[id]))
		events[i].calculateCentroid()
	return events
			
def getStats(word, date, totals):
	global database
	occurrences, = database['KeywordStatsCollection'].find({'$and': [{"date": date}, {"keyword": word}, {"bound":1}]})
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
		results = database['KeywordStatsCollection'].find({"$and":[{'date': date}, {"bound":1}]})
		for result in results:
			keywords_set.append(result['keyword'])
		print "Done creating set.\n"
		
		total_entropy = 0.0
		totals, = database['TimePeriodStatsCollection'].find({"$and":[{"date": date}, {"bound":1}]})
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
			vec = np.zeros(DIMENSION)
			total = 0.0
			zero_vec = True
			for i in range(DIMENSION):
				val = tweet['contents'].split().count(tWords[i][0])
				if val == 0:
					val = tweet['hashtags'].count(tWords[i][0])
				if val > 0:
					zero_vec = False
				total += val**2
				vec[i] = val
			#used to clean up noise....
			#if zero_vec:
			#	continue
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
			day = dt.datetime(tweet['date'].year, tweet['date'].month, tweet['date'].day, 4, 18)
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
		events[date] = cluster(vectors)
		file.write(str(date) + ": \n")
		for event in events[date]:
			event.loadTweets(ordered_tweets[date])
			file.write(str(event) + "\n\n")
	file.close()
	return events
	