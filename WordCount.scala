import org.apache.spark.SparkContext

//First Spark Program
// we need to find the frequency of each word in a file

object WordCount extends App {
  
  //Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","wordcount")    // Creating spark context
  
  val input = sc.textFile("C:/Users/hp/Downloads/search_data.txt")
  
  val words = input.flatMap(x => x.split(" "))
  
  val wordsLower = words.map(x => x.toLowerCase())    // we want to count word as 1. Removing case senstivity
  
  val wordMap = wordsLower.map(x=> (x,1))         //Tuple:- (key ->String , Value -> Int)
  
  val finalCount = wordMap.reduceByKey((a,b) => a+b)
  
  val reversedTuple = finalCount.map(x => (x._2, x._1))  // reversing the so that we can use sort function on key
  
  val sortedResults = reversedTuple.sortByKey(false).map(x => (x._2,x._1)) // again using map to get result as(String,Int)
  
  sortedResults.collect.foreach(println)

  //Add this line in your program if you want to see DAG 
  //scala.io.StdIn.readLine()

}