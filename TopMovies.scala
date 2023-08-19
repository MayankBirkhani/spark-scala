/*  Find Top movies from  from dataset movies.bat and ratings.bat
 *  Criteria:-
 *  	1. Atleast 100 people should have rated for that movie..
 *  	2. average rating > 4.5
 *  
 *  	ratings.dat columns-> user_id, movie_id, ratings_given, watched_epoch_ts
 *  	movies.bat -> movie_id, movie_name, movie_type
 *   	
 * */

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object TopMovies extends App {
 // Logger.getLogger("org").setLevel(Level.ERROR)
  
   val sc = new SparkContext("local[*]","TopMovies")
  
   val ratingsRdd = sc.textFile("D:/practice_files/ratings.dat")
  
   val mappedRdd = ratingsRdd.map(x => {
     val fields = x.split("::")
     (fields(1),fields(2))              //movie_id, ratings_given
   })
   
   //input
   //(1193,3)
   //(1193,4)
   //(1193,5)
   
   //output
   //(1193,(3.0,1.0))
   //(1193,(4.0,1.0))
   //(1193,(5.0,1.0))
   
   val newMapped = mappedRdd.mapValues(x => (x.toFloat,1.0))
   
   //Input
   //(1193,(3.0,1.0))
   //(1193,(4.0,1.0))
   //(1193,(5.0,1.0))
   
   //Output
   //(1193,(12.0,3.0))
   
   val reducedRdd = newMapped.reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))
   
   //Input
   //(1193,(12.0,3.0))
   
   val filteredRdd = reducedRdd.filter(x => x._2._2 > 100)
   
   //Input
   //(1193,(12000.0,3000.0))
   
   //Output
   //(1193,4)
   val ratingsProcessed= filteredRdd.mapValues(x => x._1/x._2).filter(x=> x._2>4.5)
   
   //ratingsProfiltered.collect.foreach(println)
   
   val moviesRdd = sc.textFile("D:/practice_files/movies.dat")
   
   val moviesMapped = moviesRdd.map(x=>{
     val fields = x.split("::")
     (fields(0),fields(1))
   })
   
   //Input
   //(101,Toy Story) --> moviesMapped
   //(101,4.7) --> ratingsProcessed
   
   val joinedRdd = moviesMapped.join(ratingsProcessed)
   
   //input
   //(101,(Toy Story,4.7)) x
   
   //output
   //x._2._1
   
   
   val topMovies = joinedRdd.map(x => x._2._1)
   
   topMovies.collect.foreach(println)
   
   scala.io.StdIn.readLine()
   
}





