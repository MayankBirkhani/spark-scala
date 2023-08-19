import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object persist extends App {
  val sc = new SparkContext("local[*]","Persist")
  
  val input = sc.textFile("C:/Users/hp/Downloads/customerorders.csv")
   
  val mappedInput = input.map(x => (x.split(",")(0),x.split(",")(2).toFloat))  // It will return tuple
  
  val totalByCustomer = mappedInput.reduceByKey((x,y)=> x+y)
 
  // Filtering customers who are paying more than 5000
  val premiumCustomers = totalByCustomer.filter(x => x._2 > 5000)
  
  // If we want to double the amount spent
  val doubledAmount = premiumCustomers.map(x => (x._1 , x._2 *2)).persist(StorageLevel.MEMORY_AND_DISK)
  //.persist(StorageLevel.MEMORY_AND_DISK)
  
  doubledAmount.collect.foreach(println)      //First Action
   
  println(doubledAmount.count)          //Second Action
  
  scala.io.StdIn.readLine()
}