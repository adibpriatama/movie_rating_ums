PROJECT_PATH="/home/adibp/projects/movie_rating_ums/data"
echo 'select * from movielens_movie.movie' | mysql -u adibp > "${PROJECT_PATH}/movie.csv"
echo 'select * from movielens_rating.rating limit 10000' | mysql -u adibp > "${PROJECT_PATH}/rating.csv"
