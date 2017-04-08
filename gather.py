import praw
import re
import os
import sqlite3
import queue
import time
import datetime
import threading

#build a queue that will later be used to control the flow of data into the database
queued = queue.Queue()
#create an authorized application to parse data on Reddit
reddit = praw.Reddit()

#create a database class to simplify the writing of SQL code. Creates a database instance
class Database(object):

	def __init__(self, dbFile):
		#takes a filename and creates a .db file from it, instantiating the database and connecting to the server
		self.conn = sqlite3.connect(dbFile+'.db')
		self.cur = self.conn.cursor()

	def entry(self, tablename, values):
		#define a method to enter data in the database
		print('Entering data to database...')
		if type(values) != type(list):
			values = list(values) #convert to a list if necessary
		self.cur.execute('INSERT INTO {} VALUES({})'.format(tablename, ','.join(['?' for value in values])),values) #automatically format data for entry into database
		self.conn.commit() #save the data to the database
		print("Succesfully submitted and saved data!")

	def query(self, tablename, search_param=None, query=None, fetchall=False, limit='*'):
		#define a method to manage the querying of the database 
		print('Querying database...')
		#sometimes there are no specific queries. Should this be the case, the database will default to selecting all data from the database
		if query is None: 
			self.cur.execute('SELECT {} FROM {}'.format(limit, tablename))
		else:
			self.cur.execute('SELECT {} FROM {} WHERE {}=?'.format(limit, tablename, search_param), [query]) #unless there is a specific query
		print('Query sucessful')
		#Depending on the arguments passed in, return either a compilation of all data or just one sample from the database
		if fetchall is False:
			return self.cur.fetchone()
		else:
			return self.cur.fetchall()

	def create_table(self, tablename, values):
		#define a method to handle the creation of databases
		#this takes care of formatting the SQL automatically
		self.cur.execute('CREATE TABLE IF NOT EXISTS {}({})'.format(tablename, ','.join(values)))
		#save the database
		self.conn.commit()
		print('Created table: {}...'.format(tablename))


#define a class to parse data on Reddit
class Parser(object):

	def __init__(self, subreddit, queue=queued, limit=None):
		#instantiate the Parser object with a subreddit, a queue object to to add to, and a limit on the number of submissions to be drawn, the default being None, which is 1000 submissions
		self.subreddit = reddit.subreddit(subreddit.lower())
		self.queue = queue
		self.limit = limit
		self.now = time.strftime('%m-%d-%Y')
		self.__call__()

	def sort_information(self, submission):
		#sorts the information that a submission object returns, keeping the data pertinent to our project and sifting out that which we do not need.
		#this returns a formatted tuple that will then go to the queue
		created = time.strftime('%m-%d-%Y', time.localtime(submission.created_utc))
		identity = submission.id
		score = submission.score
		title = submission.title
		ratio = submission.upvote_ratio
		try:
			author = str(submission.author)
		except:
			author = '[deleted]'
		return(identity, author, title, score, ratio, created, self.subreddit.display_name)

	def isAge(self, times, formatting='%m-%d-%Y'):
		#verify that the age of the post is at least 3 months
		age = datetime.datetime.strptime(self.now,formatting) - datetime.datetime.strptime(times, formatting)
		if age > datetime.timedelta(90):
			return True
		else:
			return False

	def isImage(self, url):
		#verify that the submission is in fact an image
		if re.search('(jpg)|(png)|(uploads)', url):
			return True
		else:
			return False

	def __call__(self):
		#iterate through each subreddit and check all conditions. If met, add to queue
		for submission in self.subreddit.top(limit=self.limit):
			data = self.sort_information(submission)
			if self.isAge(data[5]) and self.isImage(submission.url):
				self.queue.put(data)
				print('Submission added to queue from /r/{}'.format(data[6]))
			else:
				pass

def main():
	#orchestrate all classes and methods, as well as define categories and use the queue to funnel information
	os.system('chcp 65001')
	Database('admin').create_table('submissions',['id TEXT','author','title','score','upvote_ratio','created','subreddit'])
	categories = {'ART': 'Art','TRUMP':'The_Donald','LANDSCAPES':'EarthPorn','ANIMALS':'aww'}
	for category in categories:
		print("Parsing /r/{}".format(categories[category]))
		threading.Thread(target=Parser, kwargs={'subreddit':categories[category]}).start()
	threads = [thread for thread in threading.enumerate() if thread.is_alive()]
	while len(threads) > 1: 
		threads = [thread for thread in threading.enumerate() if thread.is_alive()]
	while not queued.empty():
		Database('admin').entry('submissions',list(queued.get()))
	results = Database('admin').query('submissions', fetchall=True)
	for line in results:
		try:
			print(line)
		except:
			print(line[0])

if __name__ == '__main__':
	#call main function
	main()
