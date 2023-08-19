import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

//Setting code breakpoints and using "Debug as" to see how code executes in background.

object DataFramesExample4 extends App{
   //Setting Log Level-> We will see only Error msgs and no info msgs.
  Logger.getLogger("org").setLevel(Level.ERROR)
   
  // Setting configuration properties
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master","local[2]")
  
  //Creating Spark Session
  val spark = SparkSession.builder()  //builder():- This method will build Spark Session
  .config(sparkConf)
  .getOrCreate()  //Spark Session is singleton, 1 application will have only 1 Spark session. If it is already created then it will "get" the session. If it is not crated so it will "create" one
  
  //Reading CSV file and loading it to Dataframe
  val ordersDF = spark.read        // read is an Action. No of Actions = No of Jobs
  .option("header",true)          // Giving option as header true, system will consider first line of data as header
  .option("inferSchema",true)      //Never use this in production. This will read few lines of data to guess the schema
  .csv("D:/practice_files/orders.csv")    
  
 // ordersDF.show()            //By default it will show 20 records. Show is also an Action
  
 // ordersDF.printSchema()     // This will print schema of Dataframe
  
  val groupedOrdersDF = ordersDF
  .repartition(4)                          //repartition is a wide transformation. repartition() method is used to increase or decrease the number of partitions of an RDD or dataframe in spark. No of Task = No of Partitions in RDD
  .where("order_customer_id > 10000")      //where is a narrow transformation.
  .select("order_id", "order_customer_id")  //select is a transformation.
  .groupBy("order_customer_id")             //groupBy is a wide transformation.
  .count()                                  //count is a transformation.
  
  groupedOrdersDF.foreach(x => {            //foreach is low level code
    println(x)
  })
  
  groupedOrdersDF.show()
  
  //Passing an info msg in Logger
  Logger.getLogger(getClass.getName).info("My Application is completed successfully")
  
  //closing Spark session
  spark.stop()
  
}