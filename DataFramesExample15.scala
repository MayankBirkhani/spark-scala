import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DateType

object DataFramesExample15 extends App {
  /*
   * I want to create a scala list
   * from the scala list I want to create a dataframe orderid, orderdate, customerid, status
   * I want to convert orderdate field to epoch timestamp (unixtimestamp) - number of seconds after 1st january 1970
   * create a new column with the name "newid" and make sure it has unique id's
   * drop duplicates - (orderdate , customerid)
   * I want to drop the orderid column
   * sort it based on orderdate
   */
  
  //Setting Log Level-> We will see only Error msgs and no info msgs.
  Logger.getLogger("org").setLevel(Level.ERROR)  
  
   // Setting configuration properties
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master","local[2]")
  
  //Creating Spark Session and for Hive integration adding property
  val spark = SparkSession.builder()  
  .config(sparkConf)
  .getOrCreate()
  
  
  // creating a scala list
  val myList = List(("1","2013-07-25 00:00:00.0","11599","CLOSED"),
                    ("2","2014-07-25 00:00:00.0","256","PENDING_PAYMENT"),
                   ("3","2013-07-25 00:00:00.0","11599","COMPLETE"),
                   ("4","2019-07-25 00:00:00.0","8827","CLOSED")
                   )
  // creating a Dataframe from the list
    val ordersDf = spark.createDataFrame(myList).toDF("orderid","orderdate","customerid","status")
    
    
  // converting order date field to epoch timestamp 
    import spark.implicits._
    val newDf = ordersDf.withColumn("orderdate", unix_timestamp(col("orderdate").cast(DateType)))
    .withColumn("newid", monotonically_increasing_id)   //created new column named "newid"& it has unique id's
    .dropDuplicates("orderdate","customerid")           //drop duplicates is orderdate and customerid are same
    .drop("orderid")                                    // drop column implemented
    .sort("orderdate")                                  // sorting data based on orderdate
    
    newDf.printSchema()
    
    newDf.show()
  
  //stopping the spark session
  spark.stop()
  
  
  
}