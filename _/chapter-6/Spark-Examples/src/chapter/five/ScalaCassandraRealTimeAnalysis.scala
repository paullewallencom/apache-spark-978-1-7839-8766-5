package chapter.five

import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._
import java.util._

/**
 *fetches the top 10 Requests from Cassandra and prints after every 5 seconds 
 */
object ScalaCassandraRealTimeAnalysis {

  def main(args: Array[String]) {
    println("Initializing TImer")
    val timer = new Timer()
    timer.schedule(new RecurringJob(), 1000,5000)
  }


  /**
   * Implementation of Timer Task
   */
  class RecurringJob extends java.util.TimerTask {

    val conf = new SparkConf()
    conf.setAppName("Apache Log Real Time Analysis using Cassandra")
    conf.set("spark.cassandra.connection.host", "localhost")
    val spCtx = new SparkContext(conf)

    def run() {
      println("Start - Print Top 10 GET request ............................ ")
      //we are using the *writetime* method of CQL which gives time(microseconds) 
      //of record written in Cassandra
      val csTimeRDD = spCtx.cassandraTable("logdata", "apachelogdata").
        select("ip", "method", "date", "method".writeTime.as("time")).where("method=?", "GET")
      csTimeRDD.collect().
        sortBy(x => calculateDate(x.getLong("time"))).reverse.take(10).foreach(
          x =>
            println(x.getString("ip") + " - " + x.getString("date") + " - " + x.getString("method") + " - " + calculateDate(x.getLong("time"))))
      println("End - Print Top 10 Latest request ............................ ")

      //The above Top-10 piece of code needs to be used with caution as it will fetch all records
      //from Cassandra table and then will perform sorting at Spark executors 
      //and will finally print the top 10 records.
      //Optimized Solution will be to perform this sorting in Cassandra itself 
      //by storing the date field in logs as timestamp in Cassandra Table and then returning 
      //Top-10 records

    }

  }
  
  /**
   * Converting Microseconds to Date
   */
  def calculateDate(data: Long): Date = {
    val cal = Calendar.getInstance
    cal.setTimeInMillis(data / 1000)
    cal.getTime
  }

}