import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DateType

/* 
 * 1. load the file and create a dataframe. I should do it using standard dataframe reader api.
 * 2. Simple Aggregate totalNumberOfRows, totalQuantity, avgUnitPrice, numberOfUniqueInvoices 
 * 3. calculate this using column object expression 
 * 4. do the same using string expression,Do it using spark sql.
 * 
 * */

object DataFramesExample16 extends App {
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
  
  // utilizing function using "column object expression"
  invoiceDf.select(
  count("*").as("RowCount"),
  sum("Quantity").as("TotalQuantity"),
  avg("UnitPrice").as("avgUnitPrice"),
  countDistinct("InvoiceDate").as("CountDistinct")
  ).show()
  
  
  // utilizing function using "string object expression"
  invoiceDf.selectExpr(
  "count(*) as RowCount",
  "sum(Quantity) as TotalQuantity",
  "avg(UnitPrice) as avgUnitPrice",
  "Count(Distinct(InvoiceDate)) as CountDistinct"
  ).show()
  
  // utilizing function using "spark sql"
  invoiceDf.createOrReplaceTempView("sales")
  
  spark.sql("select count(*) as RowCount, sum(Quantity) as TotalQuantity, avg(UnitPrice) as avgUnitPrice, count(distinct(InvoiceDate)) as CountDistinct from sales").show()
  
  
  spark.stop()
}





