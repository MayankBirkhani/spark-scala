import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

 // Implementation of Broadcast Join

object DataFramesExample22 extends App {
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
  
  val oldOrdersDF = spark.read
  .format("csv")
  .option("header",true)
  .option("path","D:/practice_files/orders_mod.csv")
  .load()
  
  //you rename the ambiguous column in one of the dataframe
  val ordersDf= oldOrdersDF.withColumnRenamed("customer_id", "cust_id")
  
   val customersDF = spark.read
  .format("csv")
  .option("header",true)
  .option("path","D:/practice_files/customers.csv")
  .load()  
  
    // Implementing Broadcast Join
  val joinedDF = ordersDf.join(customersDF,ordersDf.col("cust_id") === customersDF.col("customer_id"),"inner")
  .select("order_id", "customer_id","customer_fname")
  .sort("order_id")
  .withColumn("order_id", expr("coalesce(order_id,-1)"))  // Handling Null's in order_id
  
  joinedDF.show(50,false)
  
  spark.stop() 
}