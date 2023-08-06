CREATE EXTERNAL TABLE ums.movie_rating (
    movieId integer,
    title string, 
    year integer, 
    genres array<string>,
    rating_avg double
    )
STORED AS PARQUET
LOCATION 's3://ums-datasource/parquet_data/movie_rating/'
tblproperties ("parquet.compression"="SNAPPY");

