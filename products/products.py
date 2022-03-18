from pymongo import MongoClient
import json

Host = 'localhost' 
Port = 27017
DSN = 'mongodb://{}:{}'.format(Host,Port)
conn = MongoClient(DSN)

bd = conn['test']
try:
    coll = bd.create_collection('products')
except:
    print('Collection already exists')
    coll = bd['products']
    # delete documents if collection already exists
    coll.delete_many({})

with open('products.json', encoding='utf-8') as json_file:
    json_data = json.load(json_file)
    
coll.insert_many(json_data)


#1.  Products with category “summer”
coll.find({"categories": "summer"})
# 2. Products with “trousers” and “spring” categories
coll.find({"categories": {"$all": ["pantalones", "primavera"]}})
# 3. Products with more than 4 sizes
coll.find({"sizesStock.4": {"$exists": True}})
# 4. Products that can be bleached and cannot be ironed
coll.find({"cares.iron": False, "cares.bleach": True})
# 5. Products that are available in size m and can be ironed
coll.find({"sizesStock.m": {"$gt": 0}, "cares.iron": True})
# 6. Products with 3 or more ratings
coll.find({"ratings.2": {"$exists": True}})
# 7. User rated products ‘2’
coll.find({"ratings.user": 2})
# 8. User-rated products ‘1’ with a rating of 4 or higher
coll.find({"ratings": {"$elemMatch": {"user": 1, "rating": {"$gt": 3}}}})
# 9. Products without bleach care specification
coll.find({"cares.bleach": {"$exists": False}})
# 10. Products sorted by price ascending
coll.find({}).sort({"price": 1})
# 11. Products sorted by price descending
coll.find({}).sort({"price": -1})
# 12. Second page of 2 products sorted by description alphabetically
coll.find({}).sort({desc: 1}).skip(2).limit(2)
# 13. The 3 cheapest products
coll.find({}).sort({"price": 1}).limit(3)
# 14. The most expensive product
coll.find({}).sort({"price": -1}).limit(1)
# 15. Sort products by price in descending order and paginate the
# results with two items per page. Page 1 and
# page 2.
coll.find().sort({"price": -1}).skip(1).limit(2)
coll.find().sort({"price": -1}).skip(2).limit(2)

