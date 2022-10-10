from pymongo import MongoClient
from bson.son import SON
import datetime

client = MongoClient('mongodb://database/argo')
db = client.argo

# metadata IDs <-> cyclone selection list
agg = [
    {'$sort': {'timestamp':1}},
    {'$lookup':{'from':'tcMeta','localField':'metadata','foreignField':'_id','as':'demo'}},
    {'$group': {'_id':'$metadata', 'date':{'$first':{'$year':'$timestamp'}}, 'name':{'$first':{'$first':'$demo.name'}}}},
    {'$project': {'label': {'$concat': ['$name', ' - ', {'$toString':'$date'}] }}},
    {'$sort': {'label':1}}
]

tcs = list(db.tc.aggregate(agg))

try:
    db.summaries.replace_one({"_id": 'tc_labels'}, {"_id": 'tc_labels', "summary":tcs}, upsert=True)
except BaseException as err:
    print('error: db write failure')
    print(err)
    print(dacs)