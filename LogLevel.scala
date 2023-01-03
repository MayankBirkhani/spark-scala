import org.apache.spark.SparkContext

// Eight Spark Program

// I want to calculate the count of WARN AND ERROR

object LogLevel extends App{
  
  var sc  = new SparkContext("local[*]","wordcount")
  val myList = List(	"WARN: Tuesday 4 September 0405",
						"ERROR: Tuesday 4 September 0408",
		                "ERROR: Tuesday 4 September 0408",
		                "ERROR: Tuesday 4 September 0408",
		                "ERROR: Tuesday 4 September 0408",
		                "ERROR: Tuesday 4 September 0408"    
		                )
  
  val originalLogRdd = sc.parallelize(myList)
  
  val newPairRdd = originalLogRdd.map(x =>{
    val columns = x.split(":")
    val logLevel = columns(0)
    (logLevel,1)
  })
  
  val resultantRdd = newPairRdd.reduceByKey((x,y)=> x+y)
  
  resultantRdd.collect().foreach(println)

}