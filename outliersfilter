In this script , spark SQL sript was written to show the outiers in dataset and according to screenshot atthaced it was showed 
that no outliers seen , outliers is very sensitive issues  and it have different ways to calculate such as , the noramal distributions curve , mean absloute value 
and quantiles method , the outcomes of those methods depends on dataset and data distribuitions around average .
====================================================================================================================================

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
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
spark.conf.set("spark.sql.shuffle.partitions","5")
val requestdata = df.select("_ID","TimeSt","Country","Province","City","Latitude","Longitude")
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
val poidata = df2.select("POIID","PoiLatitude","PoiLongitude")
val crossjoindata = requestdata.crossJoin(poidata)
val distdata = crossjoindata
.withColumn("a", pow(sin(toRadians($"Latitude" - $"PoiLatitude") / 2), 2) + cos(toRadians($"PoiLatitude")) * cos(toRadians($"Latitude")) * pow(sin(toRadians($"Longitude" - $"PoiLongitude") / 2), 2))
.withColumn("distance", atan2(sqrt($"a"), sqrt(-$"a" + 1)) * 2 * 6371)
val groupdata = distdata.groupBy("POIID").agg(min("distance") as "Min_Distance",avg("distance") as "Avg_Distance" , stddev("distance") as "STD_Distance")
.withColumn("outlierfilter",expr("Min_Distance > Avg_Distance - 2 * STD_Distance") && expr("Min_Distance < Avg_Distance - 2 * STD_Distance")).show(false)

====================================================================================


