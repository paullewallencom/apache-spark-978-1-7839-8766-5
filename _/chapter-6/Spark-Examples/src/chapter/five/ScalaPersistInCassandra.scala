package chapter.five

import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import java.net.InetSocketAddress
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.dstream.DStream
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._
import chapter.four.ScalaLogAnalyzer

object ScalaPersistInCassandra {
  
  def main(args:Array[String]){
  
   /** Start Common piece of code for all kinds of Output Operations*/    
    println("Creating Spark Configuration")
    val conf = new SparkConf()
    conf.setAppName("Apache Log Persister in Cassandra")
    //Cassandra Host Name
    println("Setting Cassandra Host Name for getting Connection")
    conf.set("spark.cassandra.connection.host", "localhost")
    
    println("Retreiving Streaming Context from Spark Conf")
    val streamCtx = new StreamingContext(conf, Seconds(10))
    
    //First read the existing data from Cassandra
    readAndPrintData(streamCtx)
    
    var addresses = new Array[InetSocketAddress](1);
    addresses(0) = new InetSocketAddress("localhost",4949)
    val flumeStream = FlumeUtils.createPollingStream(streamCtx,addresses,StorageLevel.MEMORY_AND_DISK_SER_2,1000,1)
    //Utility class for Transforming Log Data
    val transformLog = new ScalaLogAnalyzer()
    //Invoking Flatmap operation to flatening the results and convert them into Key/Value pairs
    val newDstream = flumeStream.flatMap { x => transformLog.tansformLogDataIntoSeq(new String(x.event.getBody().array())) }
    /** End Common piece of code for all kinds of Output Operations*/
   
   
    //Define Keyspace
    val keyspaceName ="logdata"
    //Define Table
    val csTableName="apachelogdata"
    //Invoke saveToCassandra to persist DStream to Cassandra CF
    newDstream.saveToCassandra(keyspaceName, csTableName, SomeColumns("ip","client","user","date","method","request","protocol","respcode","size"))
    
    
    streamCtx.start();
    streamCtx.awaitTermination();  
  }
  
  /**
   * Read the data from the Cassandra and print on the Console
   */
  def readAndPrintData(streamCtx: StreamingContext){

    //Reading data from Cassandra and Printing on Console
    println("Start - Printing the data from Cassandra........... ")
    println("Start - Print All IP's ............................ ")
    //Prints the first Column (IP) of the table
    //Get the refernce of the apachelogdata table from the Context 
    //which further returns the Object of CassandraTableScanRDD 
    val csRDD = streamCtx.cassandraTable("logdata", "apachelogdata").collect()
    //Now using forEach print only the ip column using the getString() method
    csRDD.foreach ( x => println("IP = " + x.getString("ip")))
    println("End - Print All IP's ............................ ")
    println("Start - Print All Rows ............................ ")
    //Use the Same RDD and print complete Rows just by 
    //invoking toString() method
    csRDD.foreach ( x => println("Cassandra Row = " + x.toString()))
    println("End - Print All Rows ............................ ")
    println("End - Printing the data from Cassandra............ ")
    
    //Using Server Side Filters
    println("Start - Print only Filetered Rows ............................ ")
    //Get the RDD and select the column to be printed and use where clause
    //to specify the condition. 
    //Here we are selcting only "ip" column where "method=GET" 
    val csFilterRDD = streamCtx.cassandraTable("logdata", "apachelogdata").select("ip").where("method=?","GET")
    //Finally print the ip column by using foreach loop.
    csFilterRDD.collect().foreach( x => println("IP = " + x.getString("ip")))
    println("End - Print only Filetered Rows ............................ ")
    
    println("Start - Print Top 10 GET request ............................ ")
    //we are using the *writetime* method of CQL which gives time(microseconds) 
    //of record written in Cassandra
    val csTimeRDD = streamCtx.cassandraTable("logdata", "apachelogdata").
      select("ip","method","date","method".writeTime.as("time")).where("method=?","GET")
    csTimeRDD.collect().
      sortBy(x => calculateDate(x.getLong("time"))).reverse.take(10).foreach(        
        x => 
          println(x.getString("ip") + " - " + x.getString("date")+" - "+ x.getString("method")+" - "+calculateDate(x.getLong("time")) ))
    println("End - Print Top 10 Latest request ............................ ")
    
    //The above Top-10 piece of code needs to be used with caution as it will fetch all records
    //from Cassandra table and then will perform sorting at Spark executors 
    //and will finally print the top 10 records.
    //Optimized Solution will be to perform this sorting in Cassandra itself 
    //by storing the date field in logs as timestamp in Cassandra Table and then returning 
    //Top-10 records
   
  }
  
  import java.util._
  
  /**
   * Converting Microseconds to Date
   */
  def calculateDate(data:Long): Date = {
    val cal =  Calendar.getInstance
    cal.setTimeInMillis(data/1000)
    cal.getTime
  }
  
}