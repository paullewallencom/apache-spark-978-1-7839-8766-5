package chapter.one

import org.apache.spark.{SparkConf, SparkContext}

object ScalaFirstSparkExample {
  
   def main(args: Array[String]){
    
    println("Creating Spark Configuration")
    //Create an Object of Spark Configuration
    val conf = new SparkConf()
    //Set the logical and user defined Name of this Application
    conf.setAppName("My First Spark Scala Application")
    //Define the URL of the Spark Master. 
    //Useful only if you are executing Scala App directly from the console.
    //We will comment it for now but will use later
    //conf.setMaster("spark://ip-10-237-224-94:7077")

    println("Creating Spark Context")
    //Create a Spark Context and provide previously created 
    //Object of SparkConf as an reference. 
    val ctx = new SparkContext(conf)

    println("Loading the Dataset and will further process it")
    
    //Defining and Loading the Text file from the local file system or HDFS 
    //and converting it into RDD.
    //SparkContext.textFile(..) - It uses the Hadoop's TextInputFormat and file is 
    //broken by New line Character.
    //Refer to http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapred/TextInputFormat.html
    //The Second Argument is the Partitions which specify the parallelism. 
    //It should be equal or more then number of Cores in the cluster.
    val file = System.getenv("SPARK_HOME")+"/README.md";
    val logData = ctx.textFile(file, 2)

    //Invoking Filter operation on the RDD.
    //And counting the number of lines in the Data loaded in RDD.
    //Simply returning true as "TextInputFormat" have already divided the data by "\n"
    //So each RDD will have only 1 line.
    val numLines = logData.filter(line => true).count()
    
    //Finally Printing the Number of lines.
    println("Number of Lines in the Dataset " + numLines)
}


}