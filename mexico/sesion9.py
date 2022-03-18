from pymongo import MongoClient
import json
import pandas as pd

Host = 'localhost'
Port = 27017
DSN = 'mongodb://{}:{}'.format(Host, Port)
conn = MongoClient(DSN)

bd = conn['mexico']
try:
    coll = bd.create_collection('transactions')
except:
    print('Collection already exists')
    coll = bd['transactions']
    coll.drop()

df = pd.read_csv('mexico_transactions.csv', sep=';')
json_data = df.to_json(orient='records')

coll.insert_many(json.loads(json_data))

# 1. List transactions with the year, reporter, partner, and value fields only
coll.find().projection({"year": 1, "reporter": 1, "partner": 1, "value": 1})
# 2. Update the field "unit" by "liters" to all documents.
coll.updateMany({}, {"$set": {"unit": "litres"}})
# 3. Eliminate transactions between Mexico and Italy
coll.deleteMany({"partner": "Italy"})
# 4. Update all transactions where the value is equal to zero
# by placing a value of 1000.
coll.updateMany({"value": 0}, {"$set": {"value": 1000}})
# 5. List transactions destined for France only with the year and value fields.
# Sort the result by year in ascending order.
coll.aggregate([
    {"$match": {"partner": "France"}},
    {"$project": {"year": 1, "value": 1}},
    {"$sort": {"year": 1}}])
# 6. List all transactions by adding a new "qtyUnit" field indicating the weight ("wty" field)
# together with the units (“unit” field) in case wty is greater than zero. When
# wty is zero, this field should indicate "NO_INFO". Additionally, indicate the fields
# year, reporter_iso and partner_iso
coll.aggregate([
    {"$project": {"qtyUnit": {"$cond": {
        "if": {"$gt": ["$wty", 0]},
        "then": {"$concat": [{"$toString": "$wty"}, " ", "$unit"]},
        "else": "NO_INFO"
    }}, "year": 1, "reporter_iso": 1, "partner_iso": 1
    }}])
# 7. Count all transactions destined for Germany and the year 2018.
coll.aggregate([
    {"$match": {"$and": [{"partner": "Germany"}, {"year": 2018}]}},
    {"$count": "total"}])
