import json
import datetime as dt

EVENT_ID = 0
database = None

class Event:
	def __init__(self, parent_id, id, tweets):
		self.tweets = tweets
		self.id = id
		self.score = 0
		self.parent = parent_id

# This the cosine simularity function
def sim(doc1, doc2):
	score = 0.0
	for x,y in doc1,doc2:
		score += x*y
	
	return score
	
def getStats(word, date, totals):
	global database
	occurrences, = database['OccurrencesCollection'].find({'$and': [{"date": date}, {"keyword": word}]})
	from math import log10
	df = float(occurrences['df'])/totals['total_tweets']
	idf = log10(totals['total_tweets']/df)
	poh = float(occurrences['poh'])/totals['total_tweets']
	return idf, poh

# TODO: figure out a way to calculate the document count only once	
def Entropy(word, date, span):
	global database
	import math
	entro = 0.0
	tfs = []
	total_t = 0.0
	total_d = 0.0
	i = 0
	while True:
		#print date+dt.timedelta(days=i)
		#print word
		result = database['OccurrencesCollection'].find_one({'$and': [{'date':date+dt.timedelta(days=i)}, {'keyword':word}]})
		i += 1
		if not result:
			if i == span:
				break
			continue
		
		term_count = result['tf']
		tf_i = term_count
		tfs.append(tf_i)
		total_t += term_count
		if i == span:
			break
			
	for i in range(len(tfs)):
		prob_i = tfs[i]/total_t
		entro -= prob_i* math.log10(prob_i+1)
	return entro

def isTopicalword(metric, avr_metric):
		score = 0.0
		if metric[0] > avr_metric[0]:
			score += .5
		if metric[1] > avr_metric[1]:
			score += .25
		if metric[2] < avr_metric[2]:
			score += .25
		
		if score >= 0.5:
			return True
		else:
			return False
			
def getTopicalWords(date_tweet_pairs):
	global database
	keywords_set = []
	print "Creating keyword set..."
	for date in date_tweet_pairs.keys():
		results = database['OccurrencesCollection'].find({'date': date}, limit=500)
		for result in results:
			keywords_set.append(result['keyword'])
	print "Done.\n"
	
	# Structure {date: {word1: (df, poh, entro) , word2: (df, poh, entro) , ...}, ...}
	term_metrics = {}
	# Structure {date: (avr_df, avr_poh, avr_entro), ...}
	metric_averages = {}
	print "Calculating metrics..."
	for date in date_tweet_pairs.keys():
		temp = {}
		total_entropy = 0.0
		print "Calculating metrics for: ",
		print date
		totals, = database['KeywordCountCollection'].find({"date": date})
		for word in keywords_set:
			
			idf,poh = getStats(word, date, totals)
			entro = Entropy(word, date, 3)
			total_entropy += entro
			
			temp[word] = (idf, poh, entro)
		print "Done..."	
		avg_entro = total_entropy/len(keywords_set)
		
		term_metrics[date] = temp
		results, = database['KeywordCountCollection'].find({'date': date})
		avg_df = float(results['avg_df'])/results['total_tweets']
		avg_poh = float(results['avg_poh'])/results['total_tweets']
		from math import log10
		avg_idf = log10(results['total_tweets']/avg_df)
		metric_averages[date] = (avg_idf, avg_poh, avg_entro)
		
	print "Done.\n"
	file = open("Stats", 'w')
	temp = {}
	for key, value in term_metrics.items():
		temp[str(key)] = value
	file.write(json.dumps(temp, sort_keys=True, indent=4))
	
	for key, value in metric_averages.items():
		temp[str(key)] = value
	file.write(json.dumps(temp, sort_keys=True, indent=4))
	file.close()
	
	# Structure {date: [word1, word2, ...], ...}
	topicalwords = {}
	for date, word_metric_pairs in term_metrics.items():
		topicalwords[date] = []
		for word, metric in word_metric_pairs.items():
			if isTopicalword(metric, metric_averages[date]):
				topicalwords[date].append(word)
	
	return topicalwords		

def getDocVectors(topicalwords, tweets):
	# Dictionary that represents the tweets in the topicalword
	# vector space
	# Structure {date: { tweet.id: [normalized vector], ...}, ...}
	tweet_vectors = {}
	for date, tWords in topicalwords.items():
		tweet_vectors[date] = {}
		for tweet in tweets[date]:
			tweet_vectors[date][tweet['_id']] = []
			total = 0.0
			for tWord in tWords:
				val = tweet['keywords'].count(tWord)
				total += val**2
				tweet_vectors[date][tweet['_id']].append(val)
				
			# Determine the magnitude of the vector
			mag = total**.5
				
			# Normalize the vector
			for i in range(len(tweet_vectors[date][tweet['_id']])):
				if mag == 0:
					continue
				tweet_vectors[date][tweet['_id']][i] /= mag
	return tweet_vectors
	
def FindEvents(tweets, db):
	global database
	database  = db
	# Structure of tweets {date: [tweet1, tweet2, ..], ...} 
		
	# Structure {date: [word1, word2, ...], ...}
	topicalwords = getTopicalWords(tweets)
	
	# output the topicalwords to a file
	file = open("Topicalwords", 'w')
	temp = {}
	for key, value in topicalwords.items():
		temp[str(key)] = value
	file.write(json.dumps(temp, sort_keys=True, indent=4))
	file.close()
	doc_vectors = getDocVectors(topicalwords, tweets)
	
	