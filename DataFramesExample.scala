import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


// 1st Dataframe Program 
// Reading a file and creating a Dataframe out of it!!

object DataFramesExample extends App {
   
  // Setting configuration properties
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master","local[2]")
  
  //Creating Spark Session
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  //Reading CSV file and loading it to Dataframe
  val ordersDF = spark.read    
  .option("header",true)          // Giving option as header true, system will consider first line of data as header
  .option("inferSchema",true)      //Never use this in production. This will read few lines of data to guess the schema
  .csv("D:/practice_files/orders.csv")    
  
  ordersDF.show()            //By default it will show 20 records
  
  ordersDF.printSchema()     // This will print schema of Dataframe
  
  //closing Spark session
  spark.stop()
  
  
}