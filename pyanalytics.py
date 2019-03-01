import pymongo;
from pymongo import MongoClient;
import pandas as pd;
from bson.objectid import ObjectId; 
from datetime import timedelta;
from datetime import datetime;
import collections;
client = MongoClient('localhost', 27017)
db = client['emailcampaign'];
analytics = db.analytics
campaign=db.campaign


def returndatetime(timestamp):
	s = datetime.fromtimestamp(timestamp/1000);
	datetimeval = s 
	return datetimeval;

def __repr__(self):
    return str(self.__dict__) 

df = pd.DataFrame(list(campaign.find({})))


camplist = []
campids = campaign.find({},{ "_id":1,"clicks": 1,"openrate":1})
idslist = []
for x in campids:
	idslist.append(x['_id'])

hour=[]
count=[]

for ID in idslist:
	
	dbdata = campaign.find({'_id': ObjectId(ID)},{"_id":1,"clicks":1,"openrate":1})[0]
	try:
		
		df3 = pd.DataFrame(dbdata['openrate'])
		df3['distributionHour'] = df3['distribution']
		openratehour  = df3['distributionHour'][0]['hours']
		convertlist = collections.Counter(openratehour);
		openratehours =[]
		openratecount =[]
		for x,y in convertlist.items():
			openratehours.append(x)
			openratecount.append(y)
		openrateresultObj = {'campaignid':str(ID),
		'hourdistribution':{"openratehours":openratehours,"openratecount":openratecount}
		}
		
		result = db.analytics.find({'_id': ObjectId("5c761fd716fc9104e4915d1c"),'openratedistribution.campaignid':str(ID)})
		resultlist =[]

		for doc in result:
			resultlist.append(doc)

		if(len(resultlist) == 0):
			# "5c761fd716fc9104e4915d1c" ROOT TREE WANTS BE CREATED INITIALLY IN MONGO TO SET UP ANALYTICS
			db.analytics.update({'_id': ObjectId("5c761fd716fc9104e4915d1c")},{'$push': { 'openratedistribution':openrateresultObj }})
		else:
			console.log("updating")
			openrateresultObj = {"openratehours":openratehours,"openratecount":openratecount}
			db.analytics.update({'_id':ObjectId("5c761fd716fc9104e4915d1c"),'openratedistribution.campaignid':str(ID)},{'$set': {'openratedistribution.$.hourdistribution':openrateresultObj}})
	except:
		print ("error")
	try:
		val = dbdata['clicks']
		df2 = pd.DataFrame(dbdata['clicks']);
	except:
		print ("error")

	try:
		df2['readabledatetime'] = df2['lastimeclicked'].apply(lambda x : returndatetime(x));
		df2['hour'] = df2['readabledatetime'].dt.hour
		groupedhourdf2 = df2['readabledatetime'].groupby(df2['hour']).count()
		for data in groupedhourdf2.index:
			hour.append(data)
			count.append(int(groupedhourdf2.loc[data]))
			resultObj2 = {"dist":[{"hours":hour,"count":count}]}
			
		
		df['readabledatetime'] = df['timestamp'].apply(lambda x : returndatetime(x));	
		df['MONTH'] = df['readabledatetime'].apply(lambda x: int(x.month));
		df['YEAR'] = df['readabledatetime'].apply(lambda x: int(x.year));
		df['DAY'] = df['readabledatetime'].apply(lambda x: int(x.day));
		df['WEEKNUMBER'] = df['readabledatetime'].apply(lambda x: int(x.week));
		df['strWEEK'] = df['WEEKNUMBER'].apply(lambda x:str(x))
		df['strYEAR'] = df['YEAR'].apply(lambda x:str(x))
		df['strMONTH'] = df['MONTH'].apply(lambda x:str(x))
		df['MONTHYEAR'] = df['strMONTH']+'-'+df['strYEAR']
		df['WEEKYEAR'] = df['strWEEK']+'-'+df['strYEAR']
		groupeddf = df['readabledatetime'].groupby(df['MONTHYEAR']).count()
		groupedweekdf = df['readabledatetime'].groupby(df['WEEKYEAR']).count()
		x=[]
		y=[]
		xweek=[]
		yweek=[]
		for name in groupeddf.index:
		    x.append(name)
		    y.append(int(groupeddf.loc[name]))
		for number in groupedweekdf.index:
		    xweek.append(number)
		    yweek.append(int(groupedweekdf.loc[number]))    
		campaigncountresultObj = {"countbymonthandyear":{"xlabel":x,"ylabel":y},"countbyweekandyear":{"xweek":xweek,"yweek":yweek}}

		# "5c761fd716fc9104e4915d1c" ROOT TREE WANTS BE CREATED INITIALLY IN MONGO TO SET UP ANALYTICS
		db.analytics.update({'_id': ObjectId("5c790033b947840720666231")},{'$set': { 'resultObj':campaigncountresultObj }})
	except:
		a = "b"	


