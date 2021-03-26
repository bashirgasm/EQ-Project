import org.apache.spark.sql.SQLContext
import spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
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
spark.conf.set("spark.sql.shuffle.partitions","5")
val requestdata = df.select("_ID","TimeSt","Country","Province","City","Latitude","Longitude")
.dropDuplicates("TimeSt","Latitude","Longitude") // Data cleaning
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
val poidata = df2.select("POIID","PoiLatitude","PoiLongitude")
val crossjoindata = requestdata.crossJoin(poidata)
val distdata = crossjoindata
.withColumn("a", pow(sin(toRadians($"Latitude" - $"PoiLatitude") / 2), 2) + cos(toRadians($"PoiLatitude")) * cos(toRadians($"Latitude")) * pow(sin(toRadians($"Longitude" - $"PoiLongitude") / 2), 2))
.withColumn("distance", atan2(sqrt($"a"), sqrt(-$"a" + 1)) * 2 * 6371) // Distance Calculations using Haversine
val data2 =distdata.groupBy("POIID").min("distance") // Display Minimum Distance
.select("POIID","min(distance)") 
val finaldata = distdata.join(data2,distdata("POIID") === data2("POIID") && distdata("distance")===data2("min(distance)")).drop(distdata("POIID"))
.select("POIID","min(distance)") .where(col("min(distance)") > -10 && col("min(distance)") < 10)  // Display Data after Joining Distance Popularity (The target Data)
// Display Average and standard deviations data with outliers
val analysis1 =finaldata.groupBy("POIID").agg(avg("min(distance)") as "Avg(Min_Distance_Popularity)").show()
