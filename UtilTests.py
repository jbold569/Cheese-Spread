from utils import *
import datetime as dt
import unittest

class UtilityTests(unittest.TestCase):
	#def setUp(self):
		#loader = DataLoader(index=True)
		#self.DBI = dbi.DatabaseInterface()#host='hamm.cse.tamu.edu')
	
	#Make and update and test if the query returns correct results
	def testTimePeriodParser(self):  
		filename = 'geo.2012-01-01_08-51.txt.gz'
		time_period = parseTimePeriod(filename)
		expected_period = dt.datetime(2012,1,1,8,51)
		self.assertTrue(time_period == expected_period)

	def testWordFilter(self):
		test_string = "@kiana_ashleyy happy b day enjoy! test. https://twitter.com/PaulOBrien/status/282534796482199552"
		expected_words = ["happy", "day", "enjoy", "test"]
		tokens = wordFilter(test_string.split())
		msg =  "".join(tokens)
		for word in expected_words:
			if word not in  tokens:
				#msg =  msg
				self.assertFalse(True, msg=tokens)
				
		
def main():
	unittest.main()

if __name__=='__main__':
	main()

