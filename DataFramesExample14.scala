import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row


// Implementing a UDF to add a value in column basis logic.


case class Person(name:String, age:Int, city:String)

object DataFramesExample14 extends App{
  //Setting Log Level-> We will see only Error msgs and no info msgs.
  Logger.getLogger("org").setLevel(Level.ERROR)  
  
  // UDF
  def ageCheck(age:Int):String ={
    if (age > 18) "Y" else "N"
    
  }
  
  
  // Setting configuration properties
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master","local[2]")
 
  //Creating Spark Session and for Hive integration adding property
  val spark = SparkSession.builder()  
  .config(sparkConf)
  .getOrCreate()
  
  val df = spark.read
  .format("csv")
  .option("inferSchema", true)
  .option("path", "D:/practice_files/dataset1.csv")
  .load()

  import spark.implicits._
  val df1: Dataset[Row] = df.toDF("name","age","city")
  
//  val ds1 = df1.as[Person]    //DF to DS conversion by casting it to a case class.  
//  val df2 = ds1.toDF()      //DS to DF conversion
  
  val parseAgeFunction = udf(ageCheck(_:Int):String)
  val df2 = df1.withColumn("adult", parseAgeFunction(col("age"))) 

  /*
  //String/sql expression udf way of registering and calling the function and using anonymous function
  spark.udf.register("parseAgeFunction", (x:Int) => {if(x>18) "Y" else "N"})
  df1.withColumn("adult", expr("parseAgeFunction(age)"))
  */
  
  df2.show()
  
   /*
  //using UDF in sql as normal function. Uncomment above code as well to make this work
  spark.catalog.listFunctions().filter(x => x.name == "parseAgeFunction").show()
  df1.createOrReplaceTempView("peopletable")
  spark.sql("select name, age, city parseAgeFunction(age) as adult from peopletable").show()
  */
  
  
  //stopping the spark session
  spark.stop()

  
  
}