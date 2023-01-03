import org.apache.spark.SparkContext


//Third Spark Program
//how many times movies were rated 5,4,3,2,1 star
// Dataset:- user_id, movie_id, rating_given, timestamp



object RatingsCalculator extends App {
  
  val sc = new SparkContext("local[*]","wordcount")
  
  val input = sc.textFile("C:/Users/hp/Downloads/moviedata.data")
  
  val mappedInput = input.map(x => x.split("\t")(2))
  
  val results = mappedInput.countByValue
  results.foreach(println)
  
  
//  val ratings = mappedInput.map(x => (x,1))
//  
//  val reducedRatings = ratings.reduceByKey((x,y) => x+y)
//  
//  val results = reducedRatings.collect.foreach(println)
  
}