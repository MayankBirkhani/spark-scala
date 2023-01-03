import org.apache.spark.SparkContext

//Second Spark Program
//we need to find out top 10 customers who spent the maximum amount.
// Dataset :- customer_id , product_id, amount_spent

object TotalSpent extends App {
  
  val sc = new SparkContext("local[*]","wordcount")
  
  val input = sc.textFile("C:/Users/hp/Downloads/customerorders.csv")
   
  val mappedInput = input.map(x => (x.split(",")(0),x.split(",")(2).toFloat))  // It will return tuple
  
  val totalByCustomer = mappedInput.reduceByKey((x,y)=> x+y)
 
  // Filtering customers who ar epaying more than 5000
  val premiumCustomers = totalByCustomer.filter(x => x._2 > 5000)
  
  // If we want to double the amount spent
  val doubledAmount = premiumCustomers.map(x => (x._1 , x._2 * 2))
  
  val sortedTotal = totalByCustomer.sortBy(x=> x._2)
  
  val result = sortedTotal.collect
  
  result.foreach(println)
  
  // If you want to save output in a file
  // sortedTotal.saveAsTextFile("/Users/trendytech/Desktop/spark_output1")
}