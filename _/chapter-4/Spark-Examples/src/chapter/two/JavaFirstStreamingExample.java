package chapter.two;

import java.util.Arrays;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

import scala.Tuple2;

public class JavaFirstStreamingExample {
	  
	public static void main(String[] s){
	    
	    System.out.println("Creating Spark Configuration");
	    //Create an Object of Spark Configuration
	    SparkConf conf = new SparkConf();
	    //Set the logical and user defined Name of this Application
	    conf.setAppName("My First Spark Streaming Application");
	    //Define the URL of the Spark Master. 
	    //Useful only if you are executing Scala App directly from the console.
	    //We will comment it for now but will use later
	    //conf.setMaster("spark://ip-10-237-224-94:7077")
	    
	    System.out.println("Retreiving Streaming Context from Spark Conf");
	    //Retrieving Streaming Context from SparkConf Object.
	    //Second parameter is the time interval at which streaming data will be divided into batches  
	    JavaStreamingContext streamCtx = new JavaStreamingContext(conf, Durations.seconds(2));

	    //Define the the type of Stream. Here we are using TCP Socket as text stream, 
	    //It will keep watching for the incoming data from a specific machine (localhost) and port (9087) 
	    //Once the data is retrieved it will be saved in the memory and in case memory
	    //is not sufficient, then it will store it on the Disk.  
	    //It will further read the Data and convert it into DStream
	    JavaReceiverInputDStream<String> lines = streamCtx.socketTextStream("localhost", 9087,StorageLevel.MEMORY_AND_DISK_SER_2());
	    
	    //Apply the x.split() function to all elements of JavaReceiverInputDStream 
	    //which will further generate multiple new records from each record in Source Stream
	    //And then use flatmap to consolidate all records and create a new JavaDStream.
	    JavaDStream<String> words = lines.flatMap( new FlatMapFunction<String, String>() {
	    			    @Override public Iterable<String> call(String x) {
	    			      return Arrays.asList(x.split(" "));
	    			    }
	    			  });
	    		
	    
	    //Now, we will count these words by applying a using mapToPair()
	    //mapToPair() helps in applying a given function to each element in an RDD
	    //And further will return the Scala Tuple with "word" as key and value as "count".
	    JavaPairDStream<String, Integer> pairs = words.mapToPair(
	    		  new PairFunction<String, String, Integer>() {
	    		    @Override 
	    		    public Tuple2<String, Integer> call(String s) throws Exception {
	    		      return new Tuple2<String, Integer>(s, 1);
	    		    }
	    		  });
	    		
	    
	    //Further we will aggregate the value of each key by using/ applying the given function.
	    JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
	    		  new Function2<Integer, Integer, Integer>() {
	    		    @Override public Integer call(Integer i1, Integer i2) throws Exception {
	    		      return i1 + i2;
	    		    }
	    		  });
	    		
	    
	    //Lastly we will print First 10 Words.
	    //We can also implement custom print method for printing all values,
	    //as we did in Scala example.
	    wordCounts.print(10);
	    //Most important statement which will initiate the Streaming Context
	    streamCtx.start();
	    //Wait till the execution is completed.
	    streamCtx.awaitTermination();  
	  
	  }

}
