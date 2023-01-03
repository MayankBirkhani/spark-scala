import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


// 19th Example 
// We will implement Joins on:- Customer and orders dataset

object DataFramesExample19  extends App{
 
    // Setting configuration properties
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master","local[2]")
  
  
  //Creating Spark Session
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val ordersDF = spark.read
  .format("csv")
  .option("header",true)
  .option("path","D:/practice_files/orders.csv")
  .load()
  
  
   val customersDF = spark.read
  .format("csv")
  .option("header",true)
  .option("path","D:/practice_files/customers.csv")
  .load()
  
  
  // Join
  val joinedDF = ordersDF.join(customersDF,ordersDF.col("order_customer_id") === customersDF.col("customer_id"),"inner").sort("order_id")
  
  joinedDF.show
  
  spark.stop()
  
  
}