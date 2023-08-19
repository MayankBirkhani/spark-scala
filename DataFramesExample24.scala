import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

object DataFramesExample24 extends App{
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
  
  val df1 = spark.read
  .format("csv")
  .option("header", true)
  .option("path", "D:/practice_files/bigdatalog.txt")
  .load()
  
  df1.createOrReplaceTempView("logging_table")
  
  //This also works
  //val result = spark.sql("select level, date_format(datetime,'MMMM') as month, cast(date_format(datetime,'M') as Int) as month_num from logging_table").groupBy("level").pivot("month_num").count().show(10,false)
  
  //more Optimized Query by harcoding the pivot columns name so that system will not calculate distinct pivot column name. As we are confirmed that month names will not increase.
  val columns = List("January", "February", "March", "April", "May", "June", "July","August", "September", "October","November","December")

  val final_df = spark.sql("select level, date_format(datetime,'MMMM') as month from logging_table").groupBy("level").pivot("month",columns).count().show(10,false)
 
  spark.stop()
  
}


