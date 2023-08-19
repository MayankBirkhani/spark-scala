import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode

// Implementation of Spark Writer API
// Using repartition function to make part files a/c to our need.
// implementation of PartitionBy function

object DataFramesExample9 extends App{
   //Setting Log Level-> We will see only Error msgs and no info msgs.
  Logger.getLogger("org").setLevel(Level.ERROR)
   
  // Setting configuration properties
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master","local[2]")
  
  
  //Creating Spark Session
  val spark = SparkSession.builder()  
  .config(sparkConf)
  .getOrCreate()  
  
  
  //Reading CSV file using Standard way (Dataframe reader API)
  val ordersDf = spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema",true)
  .option("path","D:/practice_files/orders.csv")
  .load()
  
  print("ordersDf has "+ordersDf.rdd.getNumPartitions)
  
  val ordersRep = ordersDf.repartition(4)
  
  print("orderRep has "+ordersRep.rdd.getNumPartitions)
  
  // Writing output to a file usine write API
  ordersRep.write
  .format("csv")          // .format("json") .format("avro")
  .partitionBy("order_status")
  .mode(SaveMode.Overwrite)
  .option("maxRecordsPerFile", 2000)
  .option("path", "D:/practice_files/output")           //output will always be a directory
  .save()
  
  spark.stop()
  
}