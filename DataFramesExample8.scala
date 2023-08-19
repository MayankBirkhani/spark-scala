import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField

// Explicit Schema implementation

object DataFramesExample8 extends App {
  
  // Setting configuration properties
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master","local[2]")
  
  
  //Creating Spark Session
  val spark = SparkSession.builder()  
  .config(sparkConf)
  .getOrCreate()  
  
  // Programmatic Approach. Datatypes are of spark type
  val ordersSchema = StructType(List(
   StructField("order_id",IntegerType,false),      //false-> column cannot contain null
   StructField("order_date",TimestampType),
   StructField("order_customer_id",IntegerType),
   StructField("order_status",StringType)
  ))
  
  // DDL String Approach. Datatype will be scala type.
  val ordersSchemaDDL = "order_id Int,	order_date String,	order_customer_id Int,	order_status String"

  
  //Reading CSV file using Standard way (Dataframe reader API)
  val ordersDf = spark.read
  .format("csv")
  .option("header", true)
  .schema(ordersSchemaDDL)
  .option("path","D:/practice_files/orders.csv")
  .load()
  
  ordersDf.printSchema()
  
  ordersDf.show(false)
  
  spark.stop()
}