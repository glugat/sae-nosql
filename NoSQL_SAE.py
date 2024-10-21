import pymongo
import pandas as pd
import datetime as dt
    
URI = 'mongodb+srv://mango_user:EaHnlcFPj0coOvc9@cluster-but-sd.8r1up.mongodb.net/?retryWrites=true&w=majority&appName=cluster-but-sd'
client = pymongo.MongoClient(URI)
db = client.tp

# output the name of the collections in the database
print("Collections: ", db.list_collection_names())
print("Restaurants: ", db.restaurants)


# Question 1
print("\nQuestion 1")
q1 = db.restaurants.aggregate([
    {"$match": { "name": { "$ne": "" }}},
    {"$sortByCount" : "$name"},
    {"$limit": 10}
])
print(pd.DataFrame(list(q1)))



# Question 2
print("\nQuestion 2a")
q2a = db.restaurants.aggregate([
    {"$match": { "name": { "$ne": "" }}},
    {"$sortByCount" : "$cuisine"},
    {"$limit": 5}
])
print(pd.DataFrame(list(q2a)))

print("\nQuestion 2b")
q2b = db.restaurants.aggregate([
    {"$limit": 5},
    {"$match": { "name": { "$ne": "" }}},
    {"$sortByCount" : "$cuisine"}
])
print(pd.DataFrame(list(q2b)))

print("\nQuestion 2a (correction)")
q2a.c = db.restaurants.aggregate([
    {"$group": {"_id": "$cuisine", "nb": { "$sum": 1}}},
    {"$sort" : {"nb": -1}},
    {"$limit": 5}
])
print(pd.DataFrame(list(q2a.c)))

print("\nQuestion 2b (correction)")
q2b.c = db.restaurants.aggregate([
    {"$group": {"_id": "$cuisine", "nb": { "$sum": 1}}},
    {"$sort" : {"nb": 1}},
    {"$limit": 5}
])
print(pd.DataFrame(list(q2b.c)))



# Question 3
print("\nQuestion 3")
q3 = db.restaurants.aggregate([
    {"$sortByCount" : "$address.street"},
    {"$limit": 10}
])
print(pd.DataFrame(list(q3)))



# Question 4
print("\nQuestion 4")
q4 = db.restaurants.aggregate([
    {"$group": {
        "_id" : "$address.street",
        "quartier": {"$addToSet" : "$borough"}
    }},
    {"$addFields": {"nbQuartiers": {"$size": "$quartier"}}},
    {"$match": {"nbQuartiers": {"$gt": 2}}},
    {"$sort": {"nbQuartiers": -1}}
])
print(pd.DataFrame(list(q4)))


# Question 5
print("\nQuestion 5")
q5 = db.restaurants.aggregate([
    {"$addFields": {"premiereNote": {"$first": "$grades.score"}}},
    {"$group": {
        "_id" : "$borough",
        "nbRestaurants": {"$sum": 1},
        "scoreMoyen": {"$avg" : "$premiereNote"}
    }},
    {"$sort": {"$scoreMoyen": 1}}
])
print(pd.DataFrame(list(q5)))



# Question 6
print("\nQuestion 6")
q6 = db.restaurants.aggregate([
    {"$addFields": {"firstVisit": {"$last": "$grades.date"}, "lastVisit": {"$first": "$grades.date"}}},
    {"$group": {
         "_id": "$name",
         "firstVisit": {"$last": "$firstVisit"},
         "lastVisit": {"$first": "$lastVisit"}}}
])
print(pd.DataFrame(list(q6)))

print("\nQuestion 6 (corection)")
q6.c = db.restaurants.aggregate([
    {"$unwind": "grades"},
    {"$group": {
         "_id": "Tous",
         "début": {"$min": "$grades.date"},
         "fin": {"$max": "$grades.date"}
    }}
])
print(pd.DataFrame(list(q6.c)))





