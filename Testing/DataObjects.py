import utils

class Tweet:
	def __init__(self, dTweet):
		try:
			# Information about the tweet
			self.id = dTweet['id']
			self.retweet_count = dTweet['retweet_count']
			self.contents = dTweet['text'].lower()
			keywords = utils.WordFilter(self.contents.split())
			self.lKeywords = list(set(keywords))
			self.dTermFreqs = {}
			for word in self.keywords:
				self.dTermFreqs[word] = keywords.count(word)

			self.urls = []
			self.user_mentions = []
			self.hashtags = []

			for url in dTweet['entities']['urls']:
				self.urls.append(url['expanded_url'])
			for mention in dTweet['entities']['user_mentions']:
				self.user_mentions.append(mention['id'])
			for tag in dTweet['entities']['hashtags']:
				self.hashtags.append(tag['text'])
			#This will be a date object
			tokens = dTweet['created_at'].split(' ')
			time = tokens[3].split(':')
			self.date = datetime(int(tokens[5]), months[tokens[1]], int(tokens[2]), int(time[0]), int(time[1]), int(time[2]))
			self.valid = True

			
			if dTweet['coordinates']:
				self.location = {'type': dTweet['coordinates']['type'], 'shape': None, 'lat': dTweet['coordinates']['coordinates'][1], 'lng': dTweet['cooordinates']['coordinates'][0]}
			elif dTweet['place']:
				shape = []
				for coord in dTweet['place']['bounding_box']['coordinates'][0]:
					shape.append({'lat': coord[1], 'lng': coord[0]})
				self.location = {'type': dTweet['place']['bounding_box']['type'], 'shape': shape, 'lat': None, 'lng': None }
			else:
				self.valid = False

			self.bound = utils.assignBounds(self.location)
			
			# Information about the user
			self.user = dTweet['user']['id']
			self.follower_count = dTweet['user']['followers_count']

			except KeyError as e:
				self.valid = False
				
		def __str__(self):
			return "Location type: {0} at ({1}, {2})".format(self.location['type'], self.location['lat'], self.location['lat'])

		# This function returns a list of dictionaries of the same tweet on with
		# different keywords
		def toDBObject(self):
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
				'bound': self.bound
			}
			data = {'_id': self.id, temp_dict}
			return data

class KeywordStat:
	def __init__(self, keyword, time_period, bound=utils.UNK, poh=0)
		self.keyword = keyword
		self.time_period = time_period
		self.bound = bound
		self.poh = poh
		self.doc_freq = 0
		self.term_freq = 0
		self.entropy = []
	
	# Increases document and term frequencies simultaneously
	def incFreqs(n=1):
		self.doc_freq += 1
		self.term_freq += n
	
	def setEntropy(entro):
		self.entropy = entro
	
	
class TimePeriodStat:
	def __init__(self, time_period, bound = utils.UNK):
		self.time_period = time_period
		self.bound = bound
		self.total_hashtags = 0
		self.total_tweets = 0
		self.total_keywords = 0
	
	def incHashtags(n=1):
		self.total_hashtags += n
	
	def incTweets(n=1):
		self.total_tweets += n
		
	def incKeywords(n=1):
		self.total_keywords += n
		