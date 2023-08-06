import sys
import os
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.types import *
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.utils


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

###################################### PUT YOUR SCRIPT HERE ################################################

# config spark ketika baca s3 data pakai s3a scheme, kemungkinan tidak terpakai
spark.conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

spark.conf.set("spark.sql.legacy.timeParserPolicy","EXCEPTION")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

schema_movie = StructType([StructField('movieId', IntegerType(), False), 
                           StructField('title', StringType(), False),
                           StructField('genres', StringType(), True)])

schema_rating = StructType([StructField('userId', IntegerType(), False), 
                            StructField('movieId', IntegerType(), False), 
                            StructField('rating', DoubleType(), True),
                            StructField('timestamp', TimestampType(), True)])

filepath_movie = "s3://ums-datasource/data_airflow/movie.csv"
filepath_rating = "s3://ums-datasource/data_airflow/rating.csv"

# dataframe for movie
try:
    mdf = (spark.read.format("csv")
            .option("delimiter", "\t")
            .option("header", False)
            .option("parserLib", "univocity")
            .schema(schema_movie)
            .load(filepath_movie)
          )
except Exception as e:
    e.args += ("movie",)
    raise

# dataframe for rating
try:
    rdf = (spark.read.format("csv")
            .option("delimiter", "\t")
            .option("header", False)
            .option("parserLib", "univocity")
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .schema(schema_rating)
            .load(filepath_rating)
          )
except Exception as e:
    e.args += ("rating",)
    raise


mdf = (mdf
       .withColumn("year", F.expr("regexp_replace(reverse(split(title,' '))[0], '[()]', '')").cast(IntegerType()))
       .withColumn("title", F.expr("concat_ws(' ', slice(split(title, ' '), 1, size(split(title, ' ')) - 1))"))
       .withColumn("genres", F.expr("split(genres, '\\\|')"))
      )

rdf = rdf.groupBy("movieId").agg(F.avg("rating").alias("rating_avg"))

sdf = rdf.join(F.broadcast(mdf), on="movieId", how="inner").select(*["movieId", "title", "year", "genres", "rating_avg"])
sdf.coalesce(1).write.mode("overwrite").format("parquet").save('s3://ums-datasource/parquet_data/movie_rating/')

spark.stop()
###################################### END YOUR SCRIPT HERE ################################################
job.commit()