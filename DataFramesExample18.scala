import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.expressions.Window

// Implementing window function.

object DataFramesExample18 extends App{
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
  
  //creating a dataframe and loading the data
  val inputDf = spark.read
  .format("csv")
  .option("inferSchema", "true")
  .option("path", "D:/practice_files/windowdata.csv")
  .load()
  
  //Adding header to the dataframe data
  val invoiceDf = inputDf.toDF("country", "weeknum", "numOfInvoices", "totalQuantity", "invoiceValue")
  
  //defining window
  val myWindow= Window.partitionBy("country")
  .orderBy("weeknum")
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
  
  val finaldf = invoiceDf.withColumn("RunningTotal", sum("invoiceValue").over(myWindow))
  
  finaldf.show(20,false)
  
  spark.stop()
  
}








