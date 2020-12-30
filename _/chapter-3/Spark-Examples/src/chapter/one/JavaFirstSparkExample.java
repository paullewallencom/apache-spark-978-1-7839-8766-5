package chapter.one;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


public class JavaFirstSparkExample {

	public static void main(String[] s) {

		System.out.println("Creating Spark Configuration");
		// Create an Object of Spark Configuration
		SparkConf javaConf = new SparkConf();
		// Set the logical and user defined Name of this Application
		javaConf.setAppName("My First Spark Java Application");
		// Define the URL of the Spark Master
	    //Useful only if you are executing Scala App directly from the console.
	    //We will comment it for now but will use later
	    //conf.setMaster("spark://ip-10-237-224-94:7077");

		System.out.println("Creating Spark Context");
		// Create a Spark Context and provide previously created Object of
		// SparkConf as an reference.
		JavaSparkContext javaCtx = new JavaSparkContext(javaConf);
		System.out.println("Loading the Dataset and will further process it");
		
		//Defining and Loading the Text file from the local file system or HDFS 
	    //and converting it into RDD.
	    //SparkContext.textFile(..) - It uses the Hadoop's TextInputFormat and file is 
	    //broken by New line Character.
	    //Refer to http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapred/TextInputFormat.html
	    //The Second Argument is the Partitions which specify the parallelism. 
	    //It should be equal or more then number of Cores in the cluster.
		String file = System.getenv("SPARK_HOME")+"/README.md";
		JavaRDD<String> logData = javaCtx.textFile(file);
		
	    //Invoking Filter operation on the RDD.
	    //And counting the number of lines in the Data loaded in RDD.
	    //Simply returning true as "TextInputFormat" have already divided the data by "\n"
	    //So each RDD will have only 1 line.
		long numLines = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return true;
			}
		}).count();

		//Finally Printing the Number of lines
		System.out.println("Number of Lines in the Dataset "+numLines);

		javaCtx.close();

	}

}
