# Streaming Application to read from Kafka
# This should be the driver file for your project

# Importing required function and libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# Establishing Spark Session
spark = SparkSession  \
        .builder  \
        .appName("CapStone_Project")  \
        .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

#Creating Spark context
sc = spark.sparkContext

# Adding the required python files
sc.addPyFile('db/dao.py')
sc.addPyFile('db/geo_map.py')
sc.addFile('rules/rules.py')

# Importing all the required modules
import dao
import geo_map
import rules

# Reading data from Kafka & Topic given	
lines = spark  \
        .readStream  \
        .format("kafka")  \
        .option("kafka.bootstrap.servers","18.211.252.152:9092")  \
        .option("subscribe","transactions-topic-verified")  \
        .option("failOnDataLoss","false").option("startingOffsets", "earliest")  \
        .load()

# Schema definition to read data properly
schema =  StructType([
                StructField("card_id", StringType()),
                StructField("member_id", StringType()) ,
                StructField("amount", IntegerType()),
                StructField("pos_id", StringType()),
                StructField("postcode", StringType()),
                StructField("transaction_dt", StringType())])


# Casting raw data as string and aliasing    
Readable = lines.select(from_json(col("value") \
                                    .cast("string") \
                                    ,schema).alias("parsed"))

# Parsing the dataframe into df_parsed
df_parsed = Readable.select("parsed.*")

# Adding Time stamp column
df_parsed = df_parsed.withColumn('transaction_dt_ts',unix_timestamp(df_parsed.transaction_dt, 'dd-MM-YYYY HH:mm:ss').cast(TimestampType()))


# Function for Credit Score
def score_data(a):
	hdao = dao.HBaseDao.get_instance()
	data_fetch = hdao.get_data(key=a,table='look_up_table')
	return data_fetch['info:score']

# Defining UDF for Credit Score
Score_udf = udf(score_data,StringType())

#Adding score column
df_parsed = df_parsed.withColumn("score",Score_udf(df_parsed.card_id))

# Function for Postal Code
def postcode_data(a):
	hdao = dao.HBaseDao.get_instance()
	data_fetch = hdao.get_data(key=a,table='look_up_table')
	return data_fetch['info:postcode']

# Defining UDF for Postal Code
postcode_udf = udf(postcode_data,StringType())

# Adding Postal Code Column
df_parsed = df_parsed.withColumn("last_postcode",postcode_udf(df_parsed.card_id))


# Function for UCL
def ucl_data(a):
	hdao = dao.HBaseDao.get_instance()
	data_fetch = hdao.get_data(key=a,table='look_up_table')
	return data_fetch['info:UCL']

# Defining UDF for UCL
UCL_udf = udf(ucl_data,StringType())

# Adding UCL Column
df_parsed = df_parsed.withColumn("UCL",UCL_udf(df_parsed.card_id))


#Function for Distance calculation
def distance_calc(last_postcode,postcode):
	gmap = geo_map.GEO_Map.get_instance()
	last_lat = gmap.get_lat(last_postcode)
	last_lon = gmap.get_long(last_postcode)
	lat = gmap.get_lat(postcode)
	lon = gmap.get_long(postcode)
	final_dist = gmap.distance(last_lat.values[0],last_lon.values[0],lat.values[0],lon.values[0])
	return final_dist

# Defining UDF for Distance
distance_udf = udf(distance_calc,DoubleType())

# Adding Distance Column
df_parsed = df_parsed.withColumn("distance",distance_udf(df_parsed.last_postcode,df_parsed.postcode))

# Function for Time calculation
def time_cal(last_date, curr_date):	
	diff= curr_date-last_date
	return (diff.total_seconds())/3600

# Function for Transaction Date
def lTransD_data(a):
	hdao = dao.HBaseDao.get_instance()
	data_fetch = hdao.get_data(key=a,table='look_up_table')
	return data_fetch['info:transaction_date']

# Defining UDF for Transaction Date
lTransD_udf = udf(lTransD_data,StringType())

# Adding Transaction Date Column
df_parsed = df_parsed.withColumn("last_transaction_date",lTransD_udf(df_parsed.card_id))


# Defining UDF for Calculating Time
time_udf = udf(time_cal,DoubleType())

# Adding Time stamp column
df_parsed = df_parsed.withColumn('transaction_dt_ts',unix_timestamp(df_parsed.transaction_dt, 'dd-MM-YYYY HH:mm:ss').cast(TimestampType()))
df_parsed = df_parsed.withColumn('last_transaction_date_ts',unix_timestamp(df_parsed.last_transaction_date, 'YYYY-MM-dd HH:mm:ss').cast(TimestampType()))

# Adding Time diff column
df_parsed = df_parsed.withColumn('time_diff',time_udf(df_parsed.last_transaction_date_ts,df_parsed.transaction_dt_ts))


# Function to define the Status of the Transaction
def status_find(card_id,member_id,amount,pos_id,postcode,transaction_dt,transaction_dt_ts,last_transaction_date_ts,last_transaction_date_ts,score,distance, time_diff):
        hdao = dao.HBaseDao.get_instance()
        geo = geo_map.GEO_Map.get_instance()
        look_up = hdao.get_data(key=card_id,table='look_up_table')
        status = 'FRAUD'
        if rules.rules_check(data_fetch['info:UCL'],score,speed,amount):
		status= 'GENUINE'
		data_fetch['info:transaction_date'] = str(transaction_dt_ts)
		data_fetch['info:postcode']=str(postcode)
		hdao.write_data(card_id,data_fetch,'look_up_table')
				
        row = {'info:postcode':bytes(postcode),'info:pos_id':bytes(pos_id),'info:card_id':bytes(card_id),'info:amount':bytes(amount),
               'info:transaction_dt':bytes(transaction_dt),'info:member_id':bytes(member_id),'info:status':bytes(status)}
        key = '{0}.{1}.{2}.{3}'.format(card_id,member_id,str(transaction_dt),str(datetime.now())).replace(" ","").replace(":","")
        hdao.write_data(bytes(key),row,'card_transactions')
        return status


# Defining UDF for Status
status_udf = udf(status_find,StringType())

# Adding Status Column
df_parsed = df_parsed.withColumn('status',status_udf(df_parsed.card_id,df_parsed.member_id,df_parsed.amount,df_parsed.pos_id,df_parsed.postcode,
                                                     df_parsed.transaction_dt,df_parsed.transaction_dt_ts,df_parsed.last_transaction_date_ts,
                                                     df_parsed.score,df_parsed.distance,df_parsed.time_diff))


# Displaying only required columns
df_parsed = df_parsed.select("card_id","member_id","amount","pos_id","postcode","transaction_dt_ts","status")


# Printing Output on Console
query1 = df_parsed \
        .writeStream  \
        .outputMode("append")  \
        .format("console")  \
        .option("truncate", "False")  \
        .start()

# query termination command
query1.awaitTermination()
