import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object DataFramesExample10 extends App{
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
  .option("inferSchema",true)
  .option("path","D:/practice_files/orders.csv")
  .load()

  
  //This function will treat it as a table
  ordersDF.createOrReplaceTempView("orders")
  
  //using Spark Sql to query the data
  val resultDF = spark.sql("select order_status, count(*) as status_count from orders Group By order_status ORDER BY status_count")
  
  val resultDF2 = spark.sql("select order_customer_id, count(*) as total_orders from orders WHERE order_status='CLOSED' Group by order_customer_id Order By total_orders DESC")
  
  //resultDF.show()
  
  resultDF2.show()

  spark.stop()


}





