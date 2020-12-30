package chapter.four

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd._
import org.apache.spark.streaming.dstream._
import java.net.InetSocketAddress
import java.io.ObjectOutputStream
import java.io.ObjectOutput
import java.io.ByteArrayOutputStream
import org.apache.spark.SparkContext

object ScalaTransformLogEvents {
  

   def main(args:Array[String]){
     
    /** Start Common piece of code for all kinds of Transform operations*/    
    println("Creating Spark Configuration")
    val conf = new SparkConf()
    conf.setAppName("Apache Log Transformer")
    println("Retreiving Streaming Context from Spark Conf")
    val streamCtx = new StreamingContext(conf, Seconds(10))
    
    var addresses = new Array[InetSocketAddress](1);
    addresses(0) = new InetSocketAddress("localhost",4949)
    
    val flumeStream = FlumeUtils.createPollingStream(streamCtx,addresses,StorageLevel.MEMORY_AND_DISK_SER_2,1000,1)
        
    //Utility class for Transforming Log Data
    val transformLog = new ScalaLogAnalyzer()
    //Invoking Flatmap operation to flatening the results and convert them into Key/Value pairs
    val newDstream = flumeStream.flatMap { x => transformLog.tansformLogData(new String(x.event.getBody().array())) }
   
    /** End Common piece of code for all kinds of Transform operations*/
    
    /**Start - Transformation Functions  */
    println("Begin all Transformations")
    executeTransformations(newDstream,streamCtx)
    
    /**End - Transformation Functions  */
    
    streamCtx.start();
    streamCtx.awaitTermination();  
  }
      
   /**
    * Define and execute all Transformations to the log data
    */
  def executeTransformations(dStream:DStream[(String,String)],streamCtx: StreamingContext){
    //Start - Print all attributes of the Apache Access Log
    println("Printing all Log values")
    printLogValues(dStream,streamCtx)
    //End - Print all attributes of the Apache Access Log
    
    //Start - Will give total number of Request
    println("Printing Total Number of Request")
    dStream.filter(x=>x._1.contains("method")).count().print()
    //End - Will give total number of Request
    
    //Start - Will give total number of GET Request
    println("Printing Total number of GET request")
    dStream.filter(x=> x._1.equals("method") && x._2.contains("GET")).count().print()
    //End - Will give total number of GET Request
    
    //Start - Total number of Distinct Request
    println("Printing Total Number of Distinct Requests")
    val newStream = dStream.filter(x=>x._1.contains("request")).map(x=>(x._2,1))
    newStream.reduceByKey(_+_).print(100)
    //End - Total number of Distinct Request
    
    //Start - Transform Function
    println("Printing Transformations")
    val transformedRDD = dStream.transform(functionCountRequestType)
    streamCtx.checkpoint("checkpointDir")
    transformedRDD.updateStateByKey(functionTotalCount).print(100)
    //End - Transform Function
    
    //Start - Windowing Operation
    executeWindowingOperations(dStream,streamCtx)
    //End - Windowing Operation
    
  }
  
  
     /**
    * Window and Sliding Windows
    */
   
   def executeWindowingOperations(dStream:DStream[(String,String)],streamCtx: StreamingContext){
     
     //This Provide the Aggregated count of all response Codes
     println("Printing count of Response Code using windowing Operation")
     val wStream = dStream.window(Seconds(40),Seconds(20))
     val respCodeStream = wStream.filter(x=>x._1.contains("respCode")).map(x=>(x._2,1))
     respCodeStream.reduceByKey(_+_).print(100)
     
     //This provide the Aggregated count of all response Codes by using Window operation in Reduce method
     println("Printing count of Response Code using reducebyKeyAndWindow Operation")
     val respCodeStream_1 = dStream.filter(x=>x._1.contains("respCode")).map(x=>(x._2,1))
     respCodeStream_1.reduceByKeyAndWindow((x:Int,y:Int)=>x+y,Seconds(40),Seconds(20)).print(100)
     
     //This provide the count of Request within the duration given in the Sliding Window
     println("Applying and Printing groupByKeyAndWindow in a Sliding Window")
     val respCodeStream_2 = dStream.filter(x=>x._1.contains("respCode")).map(x=>(x._2,1))
     respCodeStream_2.groupByKeyAndWindow(Seconds(40),Seconds(20)).print(100)
     
   }
  
  
   
  //Method Borrowed from Spark Batch Application
  val functionCountRequestType = (rdd:RDD[(String,String)]) => {
     rdd.filter(f=>f._1.contains("method"))
       .map(x=>(x._2,1)).reduceByKey(_+_)
 
  }
  
  /**
   * Calculate the Total Count = Running + Current
   */
  val functionTotalCount = (values: Seq[Int], state: Option[Int])=>{
    Option(values.sum + state.sum)    
  }
  
  /**
   * Utility Function for printing all elements in the RDD's of an give DStream
   */
  def printLogValues(stream:DStream[(String,String)],streamCtx: StreamingContext){
    //Implementing ForEach function for printing all the data in provided DStream
    stream.foreachRDD(foreachFunc)
    //Define the forEachFunction and also print the values on Console
    def foreachFunc = (rdd: RDD[(String,String)]) => {
      //collect() method fetches the data from all partitions and "collects" at driver node. 
      //So in case data is too huge than driver may crash. 
      //In production environments we persist this RDD data into HDFS or use the rdd.take(n) method.
      val array = rdd.collect()
      println("---------Start Printing Results----------")
      for(dataMap<-array.array){
        print(dataMap._1,"-----",dataMap._2)
      }
      println("---------Finished Printing Results----------")
    }
  }

}