from pymongo import MongoClient
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

#connect to mongo db
Mongo_connectionstring=r"mongodb+srv://sruthygopal:sruthygopal@cluster0.yibar.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
try:
    client=MongoClient(Mongo_connectionstring, tlsAllowInvalidCertificates=True)
    print("Connected to mongo atlas success")
    #connect to mongo cluster
    db=client["sample_mflix"]
    #access a collection
    movies_collection=db['movies']
    #Query the collection
    movie_data=movies_collection.find().limit(100)

    #Define the Database
    DATABASE_URL= "sqlite:///sample_mflix.db"
    engine=create_engine(DATABASE_URL, echo=True)
    Base=declarative_base()
    SessionLocal=sessionmaker(bind=engine)

    #Define table
    class Movie(Base):
        __tablename__ = 'movies'
        id = Column(Integer, primary_key=True)
        title = Column(String(100),nullable=True)
        genre = Column(String(50),nullable=True)
        director = Column(String(50),nullable=True)
        year = Column(Integer,nullable=True)

    #Create the database and tables
    Base.metadata.create_all(engine)
    #create a new session
    session=SessionLocal()

    for movie in movie_data:
        new_movie = Movie(
            title=movie.get('title', ''),
            genre=movie.get('genre', ''),
            director=movie.get('director', ''),
            year=movie.get('year', 0)
        )
        session.add(new_movie)
    # Commit the changes to SQLite
    session.commit()

    print("Movies data added to SQLITE from MongoDB")
    
except Exception as e:
    print(e)

finally:
    client.close()        