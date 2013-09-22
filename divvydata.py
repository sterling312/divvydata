from urllib2 import urlopen
from pandas import DataFrame, Series, isnull, Panel, HDFStore
import matplotlib.pyplot as plt, numpy as np, json, MySQLdb
from datetime import datetime
from time import sleep

url = 'http://www.divvybikes.com/stations/json'

def strdatetime(string):
	d,t,m = string.split()
	d = d.split('-')
	t = t.split(':')
	if m == 'PM': t[0] = (int(t[0])+12) % 24
	return datetime(*(map(int,d+t)))

def get_df():
	web = urlopen(url).read()
	data = json.loads(web)
	timestamp = strdatetime(data[data.keys()[0]])
	data = data[data.keys()[1]]
	df = DataFrame(data)
	df['timestamp'] = timestamp
	return df, timestamp

class panel_data(object):
	def connect(self,dlist):
		self.conn = MySQLdb.Connect(*dlist)
		self.cur = self.conn.cursor()

	def close(self):
		self.cur.close()
		self.conn.close()
	
	def add_df(self):
		self.df,self.timestamp = get_df()
		return self.timestamp,self.df

	def transpose(self,id):
		foo = pd.transpose(1,0,2)
		return foo[id]
	
	def scrape(self,n=30):
		foo = []
		for i in xrange(n):
			print i	
			foo.append(self.add_df())
			sleep(60)
		self.pd = Panel(dict(foo))

	def export(self):
		store = HDFStore('hdf5/divvy.h5')
		store['divvy'] = self.pd
		store.close()

	def from_sql(self):
		self.cur.execute('SELECT * FROM divvy;')
		data = self.cur.fetchall()
		return DataFrame(np.array(data),columns=['id','location_id','latitude','longitude','timestamp','stationName','availableBikes','availableDocks'])

	def to_sql(self):
		df = self.df[['id','latitude','longitude','timestamp','stationName','availableBikes','availableDocks']]
		for i in df.index.tolist():
			self.cur.execute('INSERT INTO divvy (location_id,latitude,longitude,timestamp,stationName,availableBikes,availableDocks) VALUES (%s,%s,%s,%s,%s,%s,%s);',df.xs(i).tolist())
		self.conn.commit()

def main():
	run = panel_data()
	run.scrape()

def anls():
	store = HDFStore('divvy.h5')
	df = store['divvy']
	store.close()
	df.index = df.timestamp
	foo = map(lambda x: x[1],df.groupby('location_id'))
	for i in range(len(foo)): foo[i]['diff'] = foo[i].availableBikes.diff()
	for i in range(len(foo)): foo[i]['diff'].hist(range=[-5,5],bins=20)
	plt.show()

if __name__ == '__main__':
	main()
	anls()
