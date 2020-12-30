package chapter.six

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ScalaFirstSparkSQL {
  
  def main(Args:Array[String]){

    //Creating Spark Configuration
    val conf = new SparkConf()
    //Setting Application/ Job Name
    conf.setAppName("My First Spark SQL")
    // Define Spark Context which we will use to initialize our SQL Context 
    val sparkCtx = new SparkContext(conf)
    //Creating SQL Context
    val sqlCtx = new SQLContext(sparkCtx)
    //Define path of your JSON File (company.json) which needs to be processed 
    val path = "/home/ec2-user/softwares/company.json";
    
    //Use SQLCOntext and Load the JSON file.
    //This will return the DataFrame which can be further Queried using SQL queries.
    val dataFrame = sqlCtx.jsonFile(path)
    
    //Register the data as a temporary table within SQL Context
    //Temporary table is destroyed as soon as SQL Context is destroyed. 
    dataFrame.registerTempTable("company");
    
    //Printing the Schema of the Data loaded in the Data Frame
    dataFrame.printSchema();
    
    //Executing SQL Queries to Print all records in the DataFrame 
    println("Printing All records")
    sqlCtx.sql("Select * from company").collect().foreach(print)
    
    //Executing SQL Queries to Print Name and Employees in each Department
    println("\n Printing Number of Employees in All Departments")
    sqlCtx.sql("Select Name, No_Of_Emp from company").collect().foreach(println)
    
    //Using the aggregate function (agg) to print the 
    //total number of employees in the Company 
    println("\n Printing Total Number of Employees in Company_X")
    val allRec = sqlCtx.sql("Select * from company").agg(Map("No_Of_Emp"->"sum"))
    allRec.collect.foreach ( println )
    
    //Stop the Spark Context
    sparkCtx.stop()

  }

}