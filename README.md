Type ```hive```

```
CREATE TABLE movie_log (
    MOVIE_ID STRING,
    MOVIE_TITLE STRING,
    USER_ID STRING,
    RATINGS FLOAT,
    GENRE_ID STRING,
    RECOMMENDED STRING,
    ACTIVITY INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'p5.csv' INTO TABLE movie_log;
```


-- Write a query to select only those records which correspond to starting, browsing, completing, or
purchasing movies. Use a CASE statement to transform the RECOMMENDED column into
integers where ‘Y’ is 1 and ‘N’ is 0. Also, ensure GENREID is not null. Only include the first 25
rows.

```
SELECT USER_ID,MOVIE_ID,ACTIVITY,CASE WHEN RECOMMENDED = 'Y' THEN 1 ELSE 0
END AS RECOMMEND_INT FROM movie_log WHERE ACTIVITY IN (4,5,2,1) AND GENRE_ID IS NOT NULL LIMIT 25;
```

-- Write a query to select the customer ID, movie ID, recommended state and most recent rating for
each movie.
```
WITH MostRecentRatings AS (
    SELECT
        USER_ID,
        MOVIE_ID,
        MOVIE_TITLE,
        RATINGS,
        RECOMMENDED,
        ROW_NUMBER() OVER (PARTITION BY MOVIE_ID ORDER BY USER_ID) AS rn
    FROM movie_log
    WHERE RATINGS IS NOT NULL
)
SELECT
    USER_ID,
    MOVIE_ID,
    MOVIE_TITLE,
    CASE
        WHEN RECOMMENDED = 'Y' THEN 1
        WHEN RECOMMENDED = 'N' THEN 0
        ELSE NULL
    END AS RECOMMENDED_INT,
    RATINGS AS most_recent_rating
FROM MostRecentRatings
WHERE rn = 1
LIMIT 5;
```
