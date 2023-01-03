import org.apache.spark.SparkContext
import scala.io.Source

//Fifth and Sixth Spark Program

object KeywordAmount extends App {
  
  //This function will return Set of String types
  def loadBoringWords(): Set[String] ={
    
    var boringWords:Set[String] = Set()      //Initializing var with empty set
    
    var lines = Source.fromFile("D:/practice_files/boringwords.txt").getLines()  //opening the file locally

    //Iterating over each line and putting it to the set
    for(line <- lines){
      boringWords += line
    }
    
    boringWords            //Returning the set. Any duplicate will be ignored
  }
  
  
    
  val sc = new SparkContext("local[*]","wordcount")
  
  //Broadcasting the set on the cluster
  var nameSet = sc.broadcast(loadBoringWords)
  
  val input = sc.textFile("D:/practice_files/bigdatacampaigndata.csv")
  
  val mappedInput = input.map(x=> (x.split(",")(10).toFloat , x.split(",")(0)))
  
  
  val words = mappedInput.flatMapValues(x => x.split(" "))
  
  val finalMapped = words.map(x => (x._2.toLowerCase(),x._1))
  
  val filteredRdd = finalMapped.filter(x => !nameSet.value(x._1))    //Check if boring words are present in the data. ! means whatever is not their we will keep it, if its a match then we will ignore that.
  
  val total = filteredRdd.reduceByKey((x,y) => x+y)
  
  val sorted = total.sortBy(x=> x._2,false)
  
  //sorted.collect.foreach(println)        // Show all the records
  
  sorted.take(20).foreach(println)        // Show top 20 records
  
}