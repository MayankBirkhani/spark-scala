import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DateType

/* 
 * group the data based on Country and Invoice Number.
 * I want total quantity for each group, sum of invoice value.
 * 
 * */

object DataFramesExample17 extends App{
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
  
  val invoiceDf = spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("path", "D:/practice_files/order_data.csv")
  .load() 
  
  //Calculating total quantity for each group, sum of invoice value using column object expression.
  val summaryDf = invoiceDf.groupBy("Country", "InvoiceNo")
  .agg(sum("Quantity").as("Total Quantity"),
  sum(expr("Quantity * UnitPrice")).as("Invoice Value")
  )
  
  summaryDf.show(10,false)
  
  
  //Calculating total quantity for each group, sum of invoice value using string expression.
  val strSummaryDf =invoiceDf.groupBy("Country", "InvoiceNo")
  .agg(expr("sum(Quantity) as TotalQuantity"),
      expr("sum(Quantity * UnitPrice) as InvoiceValue")
      )
  
  strSummaryDf.show(10,false)
  
  //Calculating total quantity for each group, sum of invoice value using spark sql.
  invoiceDf.createOrReplaceTempView("invoice")
  
  val summaryDf2 =spark.sql("select Country,InvoiceNo,sum(Quantity) as TotalQuantity,sum(Quantity * UnitPrice) as InvoiceValue from invoice group by Country,InvoiceNo ")
  
  summaryDf2.show(10,false)
  
  
  spark.stop()
}