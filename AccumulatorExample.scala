import org.apache.spark.SparkContext

//Seventh Spark Program

object AccumulatorExample extends App {
  
  val sc = new SparkContext("local[*]","spacecount")
  
  val myrdd = sc.textFile("D:/practice_files/samplefile.txt")
  
  val myaccm = sc.longAccumulator("blank lines accumulator")
  
  myrdd.foreach(x => if(x=="") myaccm.add(1))
  
  println(myaccm.value)
  
}