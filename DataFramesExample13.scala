import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

// Referring columns in Dataframe, dataset and using column string, object and Expression

object DataFramesExample13 extends App{
  
  //Setting Log Level-> We will see only Error msgs and no info msgs.
  Logger.getLogger("org").setLevel(Level.ERROR)  
  
  // Setting configuration properties
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master","local[2]")
 
  //Creating Spark Session and for Hive integration adding property
  val spark = SparkSession.builder()  
  .config(sparkConf)
  .getOrCreate()
  
  val ordersDf = spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .option("path", "D:/practice_files/orders.csv")
  .load()
 
  //how to refer a column in a dataframe/dataset
  ordersDf.select("order_id", "order_status").show()
  
  import spark.implicits._
  
  //how to refer a column object in a dataframe/dataset
  ordersDf.select(column("order_id"),col("order_date"), $"order_customer_id", 'order_status).show
  
  
  //column expression examples. converting column expression to column object by using expr
  ordersDf.select(column("order_id"),col("order_date"),expr("concat(order_status,'_STATUS')")).show(false)
  
  //easier way to convert expression to column object and use column object notation
  ordersDf.selectExpr("order_id","order_date","concat(order_status,'_STATUS')").show(false)

  
  
  scala.io.StdIn.readLine()
  spark.stop()
  
  
}