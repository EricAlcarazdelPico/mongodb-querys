from pymongo import MongoClient
import json
import datetime

Host = 'localhost' 
Port = 27017
DSN = 'mongodb://{}:{}'.format(Host,Port)
conn = MongoClient(DSN)

bd = conn['test']
try:
    coll = bd.create_collection('products')
except:
    print('Collection already exists')
    coll = bd['products2']
    # delete documents if collection already exists
    coll.delete_many({})

with open('products.json', encoding='utf-8') as json_file:
    json_data = json.load(json_file)
    
for i in range(len(json_data)):
    date = json_data[i]["createdAt"].split('-')
    json_data[i]["createdAt"] = datetime.datetime(date[0], date[1], date[1])
coll.insert_many(json_data)

# 1. Get products in the "Health" category, priced between 30 and 75
# Discard fields other than name, category, and price.
# Sort the results based on price so that first we get the most expensive ones.

coll.aggregate([
    {"$match": {"$and": [{"category": "Health"}, {"price": {"$lte": 75, "$gte": 30}}]}},
    {"$project": {"name": 1, "category": 1, "price": 1}}, 
    {"$sort": {"price": -1}}])

# 2. Count the total number of documents returned by the previous aggregate.
coll.aggregate([
    {"$match": {"$and": [{"category": "Health"}, {"price": {"$lte": 75, "$gte": 30}}]}},
    {"$count": "matches"}])
    
# 3. Obtain the documents in the "Kids" category and add a new field that represents
# the price with a 10% discount.
coll.aggregate([
    {"$match": {"category": "Kids"}}, 
    {"$addFields": {"discount": {"$multiply": ["$price", 0.9]}}}])

# 4. Get the list of different categories in the collection.
coll.aggregate([
    {"$group": { _id: "$category"}}])

# 5. Calculate the total stock by category, sorting them by
# ascending form (use the stock attribute).
coll.aggregate([
    {"$group": { _id: "$category", "totalStock": {"$sum": "$stock"}}},
    {"$sort": {"totalStock": 1}}])

# 6. Calculate the average valuation of products with a price of less than 5 per category.
coll.aggregate([
    {"$match": {"price": {"$lte": 5}}},
    {"$unwind": "$ratings"},
    {"$group": { "_id": "$category", "mean": {"$avg": "$ratings.rating"}}}])
    
# 7. Calculate the sum of the prices and the average of the prices according to category,
# sorting them in ascending order by average.
coll.aggregate([
    {"$group": { "_id": "$category", "total": {"$sum": "$price"}, "mean": {"$avg": "$price"}}},
    {"$sort": {"mean": 1}}])
    
# 8. Calculate the average valuation of the products with a higher price than 20.
coll.aggregate([
    {"$match": {"price": {"$gte": 20}}}, 
    {"$unwind": "$ratings"}, 
    {"$group": { "_id": "", "mean_raiting": {"$avg": "$ratings.rating"}}}])
    