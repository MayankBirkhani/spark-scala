import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode

object DataFramesExample11 extends App{
   //Setting Log Level-> We will see only Error msgs and no info msgs.
  Logger.getLogger("org").setLevel(Level.ERROR)
   
  // Setting configuration properties
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master","local[2]")
  
  
  //Creating Spark Session and for Hive integration adding property
  val spark = SparkSession.builder()  
  .config(sparkConf)
  .enableHiveSupport()      // Spark will understand we want to use Hive Metastore
  .getOrCreate()  
  
  
  //Reading CSV file using Standard way (Dataframe reader API)
  val ordersDf = spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema",true)
  .option("path","D:/practice_files/orders.csv")
  .load()
  
  spark.sql("Create database if not exists retail")
  
  // Writing output to a file using write API
  ordersDf.write
  .format("csv")          // .format("json") .format("avro")
  .mode(SaveMode.Overwrite)
  .bucketBy(4, "order_customer_id")      //bucketBy can be used with saveAsTable
  .sortBy("order_customer_id")          // using sortBy with bucketBy for high performance
  .saveAsTable("retail.orders")

  spark.catalog.listTables("retail").show()
  
  spark.stop()
}