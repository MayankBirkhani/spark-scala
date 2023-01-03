import org.apache.spark.SparkContext


// Eight Spark Program

object LogLevelGrouping extends App {
  val sc  = new SparkContext("local[*]","wordcount")
  val baseRdd = sc.textFile("D:/practice_files/bigLog.txt")
  
  val mappedRdd = baseRdd.map(x =>{
    val fields = x.split(":")
    (fields(0),fields(1))
  })
  
  //Never Do this. Here we are just demonstrating that why we should not go with groupByKey
  // GroupByKey:- it will have all the WARN, ERROR , INFO group together in 3 partition
 // mappedRdd.groupByKey.collect().foreach(x => (x._1 , x._2.size))
  
  
  //Best to use reduceByKey
  mappedRdd.reduceByKey(_+_).collect.foreach(println)
  
}