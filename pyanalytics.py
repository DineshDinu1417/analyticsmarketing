import pymongo;
from pymongo import MongoClient;
import pandas as pd;
from bson.objectid import ObjectId; 
from datetime import timedelta;
from datetime import datetime;
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
campids = campaign.find({},{ "_id":1,"clicks": 1 })
idslist = []
for x in campids:
	idslist.append(x['_id'])

hour=[]
count=[]

for ID in idslist:
	dbdata = campaign.find({'_id': ObjectId(ID)},{"clicks":1})[0]
	try:
		df2 = pd.DataFrame(dbdata['clicks']);
		df2['readabledatetime'] = df2['lastimeclicked'].apply(lambda x : returndatetime(x));
		df2['hour'] = df2['readabledatetime'].dt.hour
		groupedhourdf2 = df2['readabledatetime'].groupby(df2['hour']).count()
		print(groupedhourdf2)
		for data in groupedhourdf2.index:
			hour.append(data)
			count.append(int(groupedhourdf2.loc[data]))
			resultObj2 = {"dist":[{"hours":hour,"count":count}]}
			print(resultObj2)
	except:
		print("Clicks not available")




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
resultObj = {"countbymonthandyear":{"xlabel":x,"ylabel":y},"countbyweekandyear":{"xweek":xweek,"yweek":yweek}}
db.analytics.update({'_id': ObjectId("5c613796c1e8c612b1d513b2")},{'$set': { 'resultObj':resultObj }})


