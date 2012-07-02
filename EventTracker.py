EVENT_ID = 0

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
	
def DocumentFrequency(word, tweets):
	occurrences = 0.0
	for tweet in tweets:
		if word in tweet['keywords']:
			occurrences += 1
	return occurrences/len(tweets), occurrences

def PoH(word, tweets):
	occurrences = 0.0
	total_tags = 0
	for tweet in tweets:
		for tag in tweet['hashtags']:
			total_tags += 1
			if word == tag.lower():
				occurrences += 1
				break
	return occurrences/total_tags
	
def Entropy(word, tweets, tf, date):
	import math
	entro = 0.0
	for i in range(24):
		occurrences = 0.0
		for tweet in tweets:
			if (tweet['date'] - date).seconds/3600 == i:
				if word in tweet['keywords']:
					occurrences += 1
		Prob_i = occurrences/tf
		entro -= Prob_i * math.log10(Prob_i+1)
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
	words = []
	print "Creating keyword set..."
	for tweets in date_tweet_pairs.values():
		for tweet in tweets:
			words += tweet['keywords']
	print "Done.\n"
	keywords_set = list(set(words))
	import json
	file = open("Keywords", 'w')
	file.write(json.dumps(keywords_set, sort_keys=True, indent=4))
	file.close()
	
	# Structure {date: {word1: (df, poh, entro) , word2: (df, poh, entro) , ...}, ...}
	term_metrics = {}
	# Structure {date: (avr_df, avr_poh, avr_entro), ...}
	metric_averages = {}
	print "Calculating metrics..."
	for date in date_tweet_pairs.keys():
		temp = {}
		metrics = [0, 0, 0]
		print "Date: ",
		print date
		
		for word in keywords_set:
			print "Calculating metrics for ",
			try:
				print word
			except UnicodeEncodeError:
				print "<Can not display word>"
			
			df,tf = DocumentFrequency(word, date_tweet_pairs[date])
			metrics[0] += df
			poh = PoH(word, date_tweet_pairs[date])
			metrics[1] += poh
			entro = Entropy(word, date_tweet_pairs[date], tf, date)
			metrics[2] += entro
			
			temp[word] = (df, poh, entro)
			print "Done..."
			
		metrics[0] /= len(keywords_set)
		metrics[1] /= len(keywords_set)
		metrics[2] /= len(keywords_set)
		
		term_metrics[date] = temp
		metric_averages[date] = tuple(metrics)
	print "Done.\n"
	print term_metrics
	print metric_averages
	file = open("Stats", 'w')
	file.write(json.dumps(term_metrics, sort_keys=True, indent=4))
	file.write(json.dumps(metric_averages, sort_keys=True, indent=4))
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
	# Structure {date: { tweet: [normalized vector], ...}, ...}
	tweet_vectors = {}
	for date, tWords in topicalwords.items():
		tweet_vectors[date] = {}
		for tweet in tweets[date]:
			tweet_vectors[date][tweet] = []
			for tWord in tWords:
				tweet_vectors[date][tweet].append(tweet['keywords'].count(tWord))
				
			# Determine the magnitude of the vector
			total = 0.0
			for val in tweet_vectors[date][tweet]:
				total += val**2
			mag = total**.5
				
			# Normalize the vector
			for i in range(len(tweet_vectors[date][tweet])):
				tweet_vectors[date][tweet][i] /= mag
	return tweet_vectors
	
def FindEvents(tweets):
	# Structure of tweets {date: [tweet1, tweet2, ..], ...} 
		
	# Structure {date: [word1, word2, ...], ...}
	topicalwords = getTopicalWords(tweets)
	
	# output the topicalwords to a file
	file = open("Topicalwords", 'w')
	import json
	file.write(json.dumps(topicalwords, sort_keys=True, indent=4))
	file.close()
	doc_vectors = getDocVectors(topicalwords, tweets)
	
	