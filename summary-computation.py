from pymongo import MongoClient
from bson.son import SON
import datetime

client = MongoClient('mongodb://database/argo')
db = client.argo

# metadata IDs <-> cyclone selection list
agg = [
    {'$sort': {'timestamp':1}},
    {'$lookup':{'from':'tcMetax','localField':'metadata','foreignField':'_id','as':'demo'}},
    {'$group': {'_id':'$metadata', 'date':{'$first':{'$year':'$timestamp'}}, 'name':{'$first':{'$first':'$demo.name'}}}},
    {'$project': {'label': {'$concat': ['$name', ' - ', {'$toString':'$date'}] }}},
    {'$sort': {'label':1}}
]

tcs = list(db.tcx.aggregate(agg))
tcs = [{'_id': doc['_id'][0], 'label': doc['label']} for doc in tcs]

try:
    db.summariesx.replace_one({"_id": 'tc_labels'}, {"_id": 'tc_labels', "summary":tcs}, upsert=True)
except BaseException as err:
    print('error: db write failure')
    print(err)
    print(dacs)

