Follow the Command

```
hdfs dfs -put ratings.csv /ratings.csv
```

Type pig


-- Task a: List All Movies and the Number of Ratings
```
ratings = LOAD '/ratings.csv' USING PigStorage(',') AS (movieId:int, title:chararray, userId:int, rating:float, genreId:int);
grouped_ratings = GROUP ratings BY movieId;
movie_rating_count = FOREACH grouped_ratings GENERATE group AS movieId, COUNT(ratings) AS numRatings;
DUMP movie_rating_count;
```

-- Task b: List All Users Who Have Rated the Same Movie and Find the Number of Ratings
```
ratings = LOAD '/ratings.csv' USING PigStorage(',') AS (movieId:int, title:chararray, userId:int, rating:float, genreId:int);
grouped_by_movie = GROUP ratings BY movieId;
user_rating_count_by_movie = FOREACH grouped_by_movie {
    users = ratings.userId;
    GENERATE group AS movieId, users, COUNT(users) AS numRatings;
};
DUMP user_rating_count_by_movie;
```

-- Task c: List All Users Who Have Rated at Least One Movie
```
unique_users = FOREACH (GROUP ratings BY userId) GENERATE group AS userId;
DUMP unique_users;
```

BACK UP
```
-- Load the ratings data
ratings = LOAD '/ratings.csv' USING PigStorage(',') 
    AS (movieId:int, title:chararray, userId:int, rating:float, genreId:int);

-- Filter movies with ratings greater than 3
high_rated_movies = FILTER ratings BY rating > 3.0;

-- Group the high-rated movies by movieId
grouped_by_movie = GROUP high_rated_movies BY movieId;

-- Count the number of movies with ratings greater than 3
movie_count = FOREACH grouped_by_movie GENERATE group AS movieId, COUNT(high_rated_movies) AS count;

-- Calculate the total count of movies with ratings greater than 3
total_count = FOREACH (GROUP movie_count ALL) GENERATE SUM(movie_count.count) AS total_movie_count;

-- Dump the result to display the total count
DUMP total_count;

```

-- Task d: Find the Count of Movies with Ratings Greater Than 3
```
ratings = LOAD '/ratings.csv' USING PigStorage(',') AS (movieId:int, title:chararray, userId:int, rating:float, genreId:int);
high_ratings = FILTER ratings BY rating > 3;
grouped_high_ratings = GROUP high_ratings BY movieId;
high_rating_count = FOREACH grouped_high_ratings GENERATE group AS movieId, COUNT(high_ratings) AS numHighRatings;
high_rating_movie_count = FILTER high_rating_count BY numHighRatings > 0;
DUMP high_rating_movie_count;
```

-- Task e: Find the Max, Min, and Average Ratings for All Movies
```
ratings = LOAD '/ratings.csv' USING PigStorage(',') AS (movieId:int, title:chararray, userId:int, rating:float, genreId:int);
grouped_ratings = GROUP ratings BY movieId;
movie_stats = FOREACH grouped_ratings GENERATE group AS movieId,
    MAX(ratings.rating) AS maxRating,
    MIN(ratings.rating) AS minRating,
    AVG(ratings.rating) AS avgRating;
DUMP movie_stats;
```

