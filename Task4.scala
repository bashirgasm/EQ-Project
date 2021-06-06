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
.dropDuplicates("TimeSt","Latitude","Longitude").show() // Data cleaning
//**************************================================================*************************************
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
val data2 =distdata.groupBy("_ID").min("distance").show() // Display Minimum Distance By ID
//**************************================================================*************************************
// Display Data after Joining (The target Data) and Filterring Data to Implement (-10 t0 10 Model  Populartity task )
val finaldata = distdata.join(data2,distdata("_ID") === data2("_ID")
 && distdata("distance")===data2("min(distance)")).drop(data2("_ID"))
.select("_ID","POIID","min(distance)").show()    // Selcting target Data   
// Calculate Average and standard deviations data with outliers
//**************************================================================*************************************
val analysis1 =finaldata.groupBy("POIID").agg(
avg("min(distance)") as "Avg(With Outliers)", stddev("min(distance)") as "STD(With Outliers)").show()
// Calculating outliers the generic outliers model (Quantiles)
val Q1 = finaldata.stat.approxQuantile("min(distance)",Array(0.25),0.05)
val Q3 = finaldata.stat.approxQuantile("min(distance)",Array(0.75),0.05)
val IQR = Q3(0) - Q1(0)
val lowerRange = Q1(0) - 1.5 *IQR
val upperRange = Q3(0)+ 1.5 *IQR
//**************************================================================*************************************
// Calculate Average and standard deviations data without outliers
// First Filterring outlier 
val analysis2 = finaldata.where(col("min(distance)")  > lowerRange  && col("min(distance)") < upperRange)
val analysis3 =analysis2.groupBy("POIID").agg(
avg("min(distance)") as "Avg(Without Outliers)", stddev("min(distance)") as "STD(Without Outliers)").show()

//**************************======================== Model Implementation ========================********************************* 

val analysis2 = finaldata.where(col("min(distance)")  > -10 && col("min(distance)") < 10)

val analysis3 =analysis2.groupBy("POIID").agg(
avg("min(distance)") as "Avg(Without Outliers)", stddev("min(distance)") as "STD(Without Outliers)").show()


//**************************================================================*************************************

//  Radious and Density with outliers Calculations
val radiousdatawith_outliers = finaldata.groupBy("POIID").agg(max("min(distance)") 
as "Radious with outliers", count("min(distance)") as "Count With outliers")
val radiousdensity = radiousdata.withColumn("densitywith(outlliers)",
radiousdata("Count With outliers")/pow(radiousdata("Radious with outliers"),2)*3.14).show()
//**************************================================================*************************************
//  Radious and Density without outliers Calculations
val radiousdatawithout_outliers = analysis2.groupBy("POIID").agg(
max("min(distance)") as "Radious without outliers", count("min(distance)") as "Count Without outliers")
val radiousdensity = radiousdatawithout_outliers.withColumn("densitywithout(outlliers)",radiousdatawithout_outliers("Count Without outliers")/pow(radiousdatawithout_outliers("Radious without outliers"),2)*3.14).show()




