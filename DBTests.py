import DatabaseInterface as dbi
from DataLoader import *
import unittest

class DatabaseTests(unittest.TestCase):
	def setUp(self):
		loader = DataLoader(index=True)
		self.DBI = dbi.DatabaseInterface()#host='hamm.cse.tamu.edu')
	
	#Make and update and test if the query returns correct results
	def testKeywordStatInsert(self):  
		self.DBI.queryKeywordStats(time_frame=ts, keyword='code', bound=dbi.USA)
		self.failUnless(False)

	def testKeywordStatInsertFail(self):
		self.failIf(False)
	
	def testTweetInsert(self):
		#load test file
		
		
		
		self.DBI.queryTweets(time_frame)
		self.failUnless(False)
		
	def testTweetInsertFail(self):
		self.failIf(False)
		
def main():
	unittest.main()

if __name__=='__main__':
	main()

