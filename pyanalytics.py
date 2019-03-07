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
		
		openratedataframe = pd.DataFrame(dbdata['openrate'])
		# openratedataframe['distributionHour'] = openratedataframe['distribution']
		# openratehour  = openratedataframe['distributionHour'][0]['hours']
		# # print(openratehour);
		openratedataframe['distributionHour'] = openratedataframe['distribution']
		openratehours = []
		for cust in (openratedataframe['distributionHour']).tolist():
			openratehours = hours + (cust['hours'])
		convertlist = collections.Counter(openratehours);
		openratehours =[]
		openratecount =[]
		for x,y in convertlist.items():
			openratehours.append(x)
			openratecount.append(y)
		openrateresultObj = {'campaignid':str(ID),
		'hourdistribution':{"openratehours":openratehours,"openratecount":openratecount}
		}

		result = db.analytics.find({'_id': ObjectId("5c790033b947840720666231"),'openratedistribution.campaignid':str(ID)})
		# print(result)
		resultlist =[]

		for doc in result:
			resultlist.append(doc)
		
		if(len(resultlist) == 0):
			# "5c761fd716fc9104e4915d1c" ROOT TREE WANTS BE CREATED INITIALLY IN MONGO TO SET UP ANALYTICS
			db.analytics.update({'_id': ObjectId("5c790033b947840720666231")},{'$push': { 'openratedistribution':openrateresultObj }})
		else:
			print("updating")
			openrateresultObj = {"openratehours":openratehours,"openratecount":openratecount}
			db.analytics.update({'_id':ObjectId("5c790033b947840720666231"),'openratedistribution.campaignid':str(ID)},{'$set': {'openratedistribution.$.hourdistribution':openrateresultObj}})
	except:
		print ("error")

		
	try:
		clickratedataframe = pd.DataFrame(dbdata['clicks']);
		clickratedataframe['readabledatetime'] = clickratedataframe['lasttimeclicked'].apply(lambda x : returndatetime(x));
		clickratedataframe['hour'] = clickratedataframe['readabledatetime'].dt.hour
		groupedhourdf2 = clickratedataframe['readabledatetime'].groupby(clickratedataframe['hour']).count()
		for data in groupedhourdf2.index:
			hour.append(data)
			count.append(int(groupedhourdf2.loc[data]))
		
		
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

		clickratedataframe['distributionHour'] = clickratedataframe['distribution']
		clickratehours = []
		for cust in (clickratedataframe['distributionHour']).tolist():
			clickratehours = clickratehours + (cust['hours'])
		
		convertlist2 = collections.Counter(clickratehours);
		print(convertlist2)
		clickratehours=[]
		clickratecounts =[]
		for x2,y2 in convertlist2.items():
			clickratehours.append(x2)
			clickratecounts.append(y2)
			clickrateresultObj = {'campaignid':str(ID),
		'hourdistribution':{"clickratehours":clickratehours,"clickratecounts":clickratecounts}
		}
		
		


		clickrateresult = db.analytics.find({'_id': ObjectId("5c790033b947840720666231"),'clickratedistribution.campaignid':str(ID)})
		
		

		clickrateresultlist =[]


		for doc in clickrateresult:
			clickrateresultlist.append(doc)


		if(len(clickrateresultlist) == 0):
			# "5c761fd716fc9104e4915d1c" ROOT TREE WANTS BE CREATED INITIALLY IN MONGO TO SET UP ANALYTICS
			db.analytics.update({'_id': ObjectId("5c790033b947840720666231")},{'$push': {'clickratemonthweek':campaigncountresultObj,'clickratedistribution':clickrateresultObj }})
		else:
			clickrateresultObj = {"clickratehours":clickratehours,"clickratecounts":clickratecounts}
			db.analytics.update({'_id':ObjectId("5c790033b947840720666231"),'clickratedistribution.campaignid':str(ID)},{'$set': {'clickratedistribution.$.hourdistribution':clickrateresultObj}})
	except:
		print ("error")



