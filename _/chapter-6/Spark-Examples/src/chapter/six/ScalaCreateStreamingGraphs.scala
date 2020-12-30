package chapter.six

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.flume._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd._
import org.apache.spark.streaming.dstream._
import java.net.InetSocketAddress
import chapter.four.ScalaLogAnalyzer


/**
 * This class shows the integration of Streams and GraphX. 
 * It creates Graphs from the the stream of events received after Applying Window Function.
 */
object ScalaCreateStreamingGraphs {
  
  /**
   * Main Method
   */
  def main(args: Array[String]) {

    //Creating Spark Configuration
    val conf = new SparkConf()
    conf.setAppName("Integrating Spark Streaming with GraphX")
    // Define Spark Context which we will use to initialize our SQL Context 
    val sparkCtx = new SparkContext(conf)
    //Retrieving Streaming Context from Spark Context
    val streamCtx = new StreamingContext(sparkCtx, Seconds(10))
    //Address of Flume Sink
    var addresses = new Array[InetSocketAddress](1);
    addresses(0) = new InetSocketAddress("localhost",4949)
    //Creating a Stream
    val flumeStream = FlumeUtils.createPollingStream(streamCtx,addresses,StorageLevel.MEMORY_AND_DISK_SER_2,1000,1)
    //Define Window Function to collect Event for a certain Duration    
    val newDstream = flumeStream.window(Seconds(40),Seconds(20))
    
    //Define Utility class for Transforming Log Data
    val transformLog = new ScalaLogAnalyzer()
    //Create Graphs for each RDD
    val graphStream = newDstream.foreachRDD { x => 
      //Invoke utility Method for Transforming Events into Graphs Vertices and Edges
      //Wrapped in a Mutable Seq
      val tuple = transformLog.transformIntoGraph(x.collect())
      println("Creating Graphs Now..................")
       //Define Vertices 
       val vertices:RDD[(VertexId, (String))] = sparkCtx.parallelize(tuple._1.toSeq)
       //Define Edges
       val edges:RDD[Edge[String]] = sparkCtx.parallelize(tuple._2.toSeq)
       //Create or Initialize Graph
       val graph = Graph(vertices,edges)
       //Print total number of Vertices and Edges in the Graph
      println("Total vertices = " + graph.vertices.count()+", Total Edges = "+graph.edges.count())
      //Printing All Vertices in the Graph
      graph.vertices.collect().iterator.foreach(f=>println("Vertex-ID = "+f._1+", Vertex-Name = "+f._2))
      
      //Printing Requests from Distinct IP's in this Window
      println("Printing Requests from Distinct IP's in this Window")
      println("Here is the Count = "+graph.vertices.filter ( x => x._2.startsWith("IP")).count())
      println("Here is the Distinct IP's = ")
      graph.vertices.filter ( x => x._2.startsWith("IP")).collect.foreach(ip=>println(ip._2))
     
      //Printing count of Distinct URL requested in this Window
      println("Printing count of Distinct URL requested in this Window")
      val filteredRDD = graph.vertices.filter ( x => x._2.startsWith("Request=")).map(x => (x._2, 1)).reduceByKey(_+_)
      filteredRDD.collect.foreach(url=>println(url._1+" = " + url._2))
      
     }
   
    streamCtx.start();
    streamCtx.awaitTermination();  
  }



}