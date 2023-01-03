import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger


// 2nd & 3rd Dataframe Program 
// Implementing:- Select, where, groupBy etc in a data

object DataFramesExample2 extends App {
  
  // Setting Logging Level. This line will let our code only show error messages
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  // Setting configuration properties
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master","local[2]")
  
  
  //Creating Spark Session
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  

  // DataFrame creation
  val ordersDF = spark.read
  .option("header",true)
  .csv("D:/practice_files/orders.csv")
  
  
  val groupedByOrdersDF = ordersDF
  .repartition(4)
  .where("order_customer_id > 10000")
  .select("order_id","order_customer_id")
  .groupBy("order_customer_id")
  .count()
  
  groupedByOrdersDF.show()
  
  //Logging 
 // Logger.getLogger(getClass.getName).info("My Application is completed successfully")
  
  
  spark.stop()
  
}