package chapter.five

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
import chapter.four.ScalaLogAnalyzer
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop._

/**
 * @author sagupta
 */
object ScalaPersistLogData {

   def main(args:Array[String]){
     
    /** Start Common piece of code for all kinds of Output Operations*/    
    println("Creating Spark Configuration")
    val conf = new SparkConf()
    conf.setAppName("Apache Log Persister")
    println("Retreiving Streaming Context from Spark Conf")
    val streamCtx = new StreamingContext(conf, Seconds(10))
    
    var addresses = new Array[InetSocketAddress](1);
    addresses(0) = new InetSocketAddress("localhost",4949)
    
    val flumeStream = FlumeUtils.createPollingStream(streamCtx,addresses,StorageLevel.MEMORY_AND_DISK_SER_2,1000,1)
        
    //Utility class for Transforming Log Data
    val transformLog = new ScalaLogAnalyzer()
    //Invoking Flatmap operation for flatening the results and convert them into Key/Value pairs
    val newDstream = flumeStream.flatMap { x => transformLog.tansformLogData(new String(x.event.getBody().array())) }
   
    /** End Common piece of code for all kinds of Output Operations*/
    
    /**Start - Output Operations */
    persistsDstreams(newDstream,streamCtx)
    /**End - Output Operations  */
    
    streamCtx.start();
    streamCtx.awaitTermination();  
  }
      
   /**
    * Define and execute all Output Operations over DStreams
    */
  def persistsDstreams(dStream:DStream[(String,String)],streamCtx: StreamingContext){
    
   //Writing Data as Text Files on Local File system. 
   //This method takes 2 arguments: -
   //1."prefix" of file, which would be appended with Time (in milliseconds) by Spark API's 
   //2."suffix" of the file
   //The final format will be "<prefix><Milliseconds><suffix>"
   dStream.saveAsTextFiles("/home/ec2-user/softwares/spark-1.3.0-bin-hadoop2.4/outputDir/data-", "")
    
   //Creating an Object of Hadoop Config with default Values
   val hConf = new JobConf(new org.apache.hadoop.conf.Configuration())
   
   //Defining the TextOutputFormat using old Api's available with =<0.20 
   val oldClassOutput = classOf[org.apache.hadoop.mapred.TextOutputFormat[Text,Text]]
   //Invoking Output operation to save data in HDFS using using oldApi's 
   //This method accepts following Parameters: -
   //1."prefix" of file, which would be appended with Time (in milliseconds) by Spark API's 
   //2."suffix" of the file
   //3.Key - Class which can work with the Key  
   //4.Value - Class which can work with the Key
   //5.OutputFormat - Class needed for writing the Output in a sepcific Format
   //6.HadoopConfig - Object of Hadoop Config
   dStream.saveAsHadoopFiles("hdfs://localhost:9000/spark/streaming/oldApi/data-", "", classOf[Text], classOf[Text], oldClassOutput ,hConf )
   
   //Defining the TextOutputFormat using new Api's available with >0.20
   val newTextOutputFormat = classOf[org.apache.hadoop.mapreduce.lib.output.TextOutputFormat[Text, Text]]
   
   //Invoking Output operation to save data in HDFS using using new Api's 
   //This method accepts same set of parameters as "saveAsHadoopFiles"
   dStream.saveAsNewAPIHadoopFiles("hdfs://localhost:9000/spark/streaming/newApi/data-", "", classOf[Text], classOf[Text], newTextOutputFormat ,hConf )
   
    //Defining saveAsObject for saving data in form of Hadoop Sequence Files
    dStream.saveAsObjectFiles("hdfs://localhost:9000/spark/streaming/sequenceFiles/data-")
    
   //Using forEachRDD for printing the data for each Partition
    dStream.foreachRDD( 
        rdd => rdd.foreachPartition(
        data=> data.foreach(
            //Printing the Values which can be replaced by custom code 
            //for storing data in any other external System.
            tup => System.out.println("Key = "+tup._1+", Value = "+tup._2)
            )   
        )    
    )
   
  }

}

   