import cherrypy
import json

class Test(object):

	@cherrypy.expose
	def index(self):
		return "hello world"

	@cherrypy.expose
	def query(self, keywords, startDate):
		print "\n\n"
		print keywords
		print startDate
		return json.dumps("Hello")

cherrypy.quickstart(Test(), config="etc/web.conf")