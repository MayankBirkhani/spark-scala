import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

// Reading data from unstructured file using RDD

object DataFramesExample12 extends App {
  
  //Setting Log Level-> We will see only Error msgs and no info msgs.
  Logger.getLogger("org").setLevel(Level.ERROR)   
  
  val myregex = """^(\S+) (\S+)\t(\S+)\,(\S+)""".r
  
  //This will add structure to our regex result line
  case class Orders(order_id:Int,customer_id:Int,order_status:String)
  
  //Taking raw line as input and giving structured line as output:-
  def parser(line:String )={
    line match{
      case myregex(order_id, date, customer_id, order_status)=>
        Orders(order_id.toInt, customer_id.toInt, order_status)
    }
  }
  
  // Setting configuration properties
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master","local[2]")
  
  //Creating Spark Session
  val spark = SparkSession.builder()  
  .config(sparkConf)
  .getOrCreate()
  
  //Creating a raw RDD where every line will be read as a String
  val lines = spark.sparkContext.textFile("D:/practice_files/orders_new.csv")
  
  import spark.implicits._
  
  //instead of writing a anonymous function we are writing a modular function.
  val ordersDS = lines.map(parser).toDS().cache()
  
  ordersDS.printSchema()
  ordersDS.select("order_id").show()
  ordersDS.groupBy("order_status").count().show()
  
  scala.io.StdIn.readLine()
  spark.stop()

}




