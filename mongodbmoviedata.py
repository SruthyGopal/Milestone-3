from pymongo import MongoClient
import sqlite3

connection_string=r"mongodb+srv://sruthygopal:sruthygopal@cluster0.yibar.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
sqlite_connection=sqlite3.connect(r'C:\Users\Administrator\Desktop\UST_Training\Assessment2\Chinook_Sqlite.sqlite')
sqlite_cursor= sqlite_connection.cursor()

try:
    client=MongoClient(connection_string, tlsAllowInvalidCertificates=True)
    print("Connected to mongo atlas success")
    #print(client.list_database_names())

    #connect to mongo cluster
    db=client["sample_mflix"]

    #access a collection
    collection=db['movies']

    #Query the collection
    results=collection.find().limit(100)

    sqlite_cursor.execute('''
    CREATE TABLE IF NOT EXISTS movies (
        _id TEXT PRIMARY KEY,
        plot TEXT,
        genre TEXT,
        runtime INTEGER,
        cast TEXT,
        languages TEXT
    )
''')
    for movie in results:
        movie_data={
            "plot":movie["plot"],
            "genre":movie["genre"],
            "runtime":movie["runtime"],
            "cast":movie["cast"],
            "languages":movie["languages"]
        }
   
        sqlite_cursor.execute('''
        INSERT OR REPLACE INTO movies (_id, plot, genre, runtime, cast, languages)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (movie_data['_id'], movie_data['title'], movie_data['director'], movie_data['release_year'], movie_data['genre'], movie_data['rating']))
    sqlite_connection.commit()

    print("Data inserted from MongoDB to SQLite")

except Exception as e:
    print(e)    

finally:
    sqlite_connection.close()
    client.close()