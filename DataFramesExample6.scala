import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import java.sql.Timestamp

// Practical to check difference between Dataset and dataframe

//Converting Dataframe to Dataset using case class.
case class OrdersData(order_id: Int,	order_date: Timestamp,	order_customer_id: Int,	order_status: String)

object DataFramesExample6 extends App{
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
  
  
  //Reading CSV file and loading it to Dataframe
  val ordersDF = spark.read        
  .option("header",true)          
  .option("inferSchema",true)     
  .csv("D:/practice_files/orders.csv") 

  //This import is required to convert datsafrsme to data set or dataset to dataframe.
  //This has to be import after Spark Session
  
  import spark.implicits._
  
  val ordersDS = ordersDF.as[OrdersData]    //Casting DF to specific (object type) Dataset

//  ordersDS.filter(x => x.order_ids < 10)    // we will get compile time error while dealing with Dataset
  
 // ordersDF.filter("order_ids < 10")        // we will get runtime error while dealing with Dataframe


}








