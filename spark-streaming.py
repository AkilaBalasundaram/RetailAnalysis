#Import all the libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#python Function to calculate total number of items present in single order

def item_count(items):
	count=0
	for item in items:
		count=count+item['quantity']
	return count


#python Function to calculate total cost of items present in single order

def item_price(items,type):
    totalcost=0
    if type=="ORDER":
        for item in items:
            totalcost=totalcost+(item['quantity']*item['unit_price'])
        return totalcost
    else:
        for item in items:
            totalcost=totalcost+(item['quantity']*item['unit_price'])
        return totalcost*(-1)

#python function to set if its order /reorder

def order_return1(type):
    if type=="ORDER":
        return 1
    else:
        return 0
def order_return2(type):
    if type=="RETURN":
        return 1
    else:
        return 0

# Define the spark session
spark = SparkSession  \
        .builder  \
        .appName("StructuredSocketRead")  \
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Read input from Kafka topic "real-time-project"
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
  .option("startingOffsets","latest") \
  .option("subscribe", "real-time-project") \
  .load()


# Define Schema of Single Order
jsonSchema= StructType()\
        .add("invoice_no",StringType()) \
        .add("country",StringType()) \
        .add("timestamp",StringType()) \
        .add("type",StringType()) \
        .add("items",ArrayType(StructType([
            StructField("SKU",StringType()),
            StructField("title",StringType()),
            StructField("unit_price",DoubleType()),
            StructField("quantity",IntegerType()),
            ])))

#flatten out the data and give a alias name "data"

orderStream=df.select(from_json(col("value").cast("string"),jsonSchema).alias("data")).select("data.*")
#define the UDF with the python function

total_item_count=udf(item_count,IntegerType()) #UDF function for total number of items in a order
total_item_cost=udf(item_price,DoubleType())   #UDF function for total price of items in a order
is_order=udf(order_return1,IntegerType())      #sets 1 if transaction is order else 0
is_return=udf(order_return2,IntegerType())     #sets 1 if transaction is return else 0


#calculate additional columns using define UDF functions by passing required parameters
expandedOrderStream=orderStream \
			.withColumn("total_items",total_item_count(orderStream.items)) \
                        .withColumn("total_cost",total_item_cost(orderStream.items,orderStream.type))\
                        .withColumn("is_order",is_order(orderStream.type)) \
                        .withColumn("is_return",is_return(orderStream.type))\
                        .withColumn("timestamp",current_timestamp()) \


#Print resultant data into console window data into console and the table is generated for 1 minute
query=expandedOrderStream \
       . select("invoice_no","country","timestamp","total_items","total_cost","is_order","is_return") \
       .writeStream    \
       .outputMode("append") \
       .format("console") \
       .option("truncate","false") \
       .trigger(processingTime="1 minute")     \
       .start()  \


# calculate Time,country based KPI's by grouping each order(time,country) and calcuating aggregated columns
aggOrderByTimeCountry=expandedOrderStream \
                .withWatermark("timestamp","1 minute")   \
                .groupBy(window("timestamp","1 minute","1 minute"),"country")    \
                .agg(sum("total_cost").alias("total_sale_volume"), #Total volume of sales
                        count("invoice_no").alias("OPM"), #Order per minute
                        (avg("is_return")*100).alias("rate_of_return"))\
                 .select("window","country","OPM","total_sale_volume","rate_of_return")


# write the stream into JSON file format at a tumbling window of 1 minute on orders on per-country basis and store in HDFS under /user/CP1
queryByTimeCountry=aggOrderByTimeCountry.writeStream   \
        .format("json") \
        .outputMode("append")   \
        .option("truncate","false") \
        .option("path","/user/op1") \
        .option("checkpointLocation","/user/cp1")\
        .trigger(processingTime="1 minute")  \
        .start()\
        


# calculate Time based KPI's by grouping each order(time) and calcuating aggregated columns
aggOrderByTime=expandedOrderStream \
                .withWatermark("timestamp","1 minute")   \
                .groupBy(window("timestamp","1 minute","1 minute"))    \
                .agg(sum("total_cost").alias("total_sale_volume"), #Total volume of sales
                        count("invoice_no").alias("OPM"),#Order per minute
                        (avg("is_return")*100).alias("rate_of_return"), # Rate of Return
                        ((sum("total_cost"))/(sum("is_order")+sum("is_return"))).alias("average_transaction_size"))  \
                .select("window","OPM","total_sale_volume","average_transaction_size","rate_of_return")


# write the stream into JSON file format at a tumbling window of 1 minute on orders across globe and store in HDFS under /user/CP2

queryByTime=aggOrderByTime.writeStream   \
        .format("json") \
        .outputMode("append")   \
        .option("truncate","false") \
        .option("path","/user/op2") \
        .option("checkpointLocation","/user/cp2") \
        .trigger(processingTime="1 minute") \
        .start() \


# Wait until any of the queries on the associated SQLContext has terminated 
spark.streams.awaitAnyTermination()
