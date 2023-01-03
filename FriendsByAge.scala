import org.apache.spark.SparkContext

//Fourth Spark Program

object FriendsByAge extends App{
  
  def parseLine(line:String) ={
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age,numFriends)
  }
  
  val sc = new SparkContext("local[*]","wordcount")
  
  val input = sc.textFile("C:/Users/hp/Downloads/friendsdata.csv")
  
  val mappedInput = input.map(parseLine)
  
//val mappedFinal = mappedInput.map(x => (x._1,(x._2,1)))
  
  val mappedFinal = mappedInput.mapValues(x => (x,1))
  
  val totalsByAge = mappedFinal.reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))
  
//val averagesByAge = totalsByAge.map(x => (x._1, x._2._1/x._2._2)).sortBy(x=> x._2)
  
  val averagesByAge = totalsByAge.mapValues(x => x._1/x._2).sortBy(x=> x._2)
  
  
  averagesByAge.collect.foreach(println)
  
}