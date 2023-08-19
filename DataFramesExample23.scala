import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

// Group the data on the basis of level and month name. 
// Challenge:- Month name is directly not given


object DataFramesExample23 extends App{
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
  
  val result = spark.sql("select level, date_format(datetime,'MMMM') as month, cast(first(date_format(datetime,'M')) as Int) as month_num, count(1) as total from logging_table group by level,month order by month_num, level")
  
  val final_result = result.drop("month_num").show(60,false)
  
  spark.stop()
  
}









