import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{window,col,column}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,DoubleType}
val sqlContext = new SQLContext(sc)
val requestSchema = StructType(Array(
    StructField("_ID", IntegerType, true),
    StructField("TimeSt", StringType, true),
    StructField("Country", StringType, true),
    StructField("Province", StringType, true),
    StructField("City", StringType, true),
    StructField("Latitude", DoubleType, true),
    StructField("Longitude", DoubleType, true)))
val df = spark.read
    .format("csv")
    .option("header", "true") 
    .schema(requestSchema )
    .load("/home/spark/Downloads/DataSample.csv")
df.createOrReplaceTempView("requestView")
spark.conf.set("spark.sql.shuffle.partitions","5")
val requestdata =
spark.sql("""
select * from requestView
""").dropDuplicates("TimeSt","Latitude","Longitude")
 df.select("_ID","TimeSt","Country","Province","City","Latitude","Longitude")
.dropDuplicates("TimeSt","Latitude","Longitude")
val poischema = StructType(Array(
    StructField("POIID", StringType, true),
    StructField("PoiLatitude", StringType, true),
    StructField("PoiLongitude", StringType, true)
   ))
val df2 = spark.read
    .format("csv")
    .option("header", "true") 
    .schema(poischema )
    .load("/home/spark/Downloads/POIList.csv")
df2.createOrReplaceTempView("poiView")
val poidata = spark.sql("""
select  * from  poiView
""")
val crossjoindata = requestdata.crossJoin(poidata)
//crossjoindata.createOrReplaceTempView("requestView")
val distdata = crossjoindata
.withColumn("a", pow(sin(toRadians($"Latitude" - $"PoiLatitude") / 2), 2) + cos(toRadians($"PoiLatitude")) * cos(toRadians($"Latitude")) * pow(sin(toRadians($"Longitude" - $"PoiLongitude") / 2), 2))
.withColumn("distance", atan2(sqrt($"a"), sqrt(-$"a" + 1)) * 2 * 6371)
.groupBy("POIID").agg(avg("distance")as "Avg_Distance",stddev("distance") as "STD_Distance")// Task3
.show(false)
.groupBy("_ID").agg(min("distance")as "Min_Distance").show(false)// Task1+2
