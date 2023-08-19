import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger


//Reading different format files using Spark dataframe reader API

object DataFramesExample7 extends App{
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
  
  
  //Reading CSV file using Standard way (Dataframe reader API)
  val ordersDf = spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema",true)
  .option("path","D:/practice_files/orders.csv")
  .load()
  
  val playerDf = spark.read
  .format("json")
  .option("path","D:/practice_files/players.json")
  .option("mode", "DROPMALFORMED")
  .load()
  
  val usersDF = spark.read
  .option("path", "D:/practice_files/users.parquet")  // we don't need to specify format for parquet files, as it is default file format in spark.
  .load()
  
  usersDF.printSchema()
  
  usersDF.show(false)          // false means we are setting truncate mode as false. It will show full data of column

  spark.stop()

}













