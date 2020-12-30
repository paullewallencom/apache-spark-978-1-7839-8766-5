package chapter.six

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.rdd.RDD

object ScalaSparkGraphx {

  def main(args: Array[String]) {

    //Creating Spark Configuration
    val conf = new SparkConf()
    conf.setAppName("My First Spark Graphx")
    // Define Spark Context which we will use to initialize our SQL Context 
    val sparkCtx = new SparkContext(conf)

    //Define Vertices/ Nodes for Subjects
    //"parallelize()" is used to distribute a local Scala collection to form an RDD.
    //It acts lazily, i.e. 
    //if a mutable collection is altered after invoking "parallelize" but before
    //any invocation to the RDD operation then your resultant RDD will contain modified collection.
    val subjects: RDD[(VertexId, (String))] = sparkCtx.parallelize(Array((1L, ("English")), (2L, ("Math")),(3L, ("Science"))))

    //Define Vertices/ Nodes for Teachers
    val teachers: RDD[(VertexId, (String))] = sparkCtx.parallelize(Array((4L, ("Leena")), (5L, ("Kate")),(6L, ("Mary"))))
    //Define Vertices/ Nodes for Students
    val students: RDD[(VertexId, (String))] = sparkCtx.parallelize(Array((7L, ("Adam")), (8L, ("Joseph")),(9L, ("Jessica")),(10L, ("Ram")),(11L, ("brooks")),(12L, ("Camily"))))
    //Join all Vertices and create 1 Vertice
    val vertices = subjects.union(teachers).union(students)
    
    //Define Edges/ Relationships between Subject vs Teachers
    val subjectsVSteachers: RDD[Edge[String]] = sparkCtx.parallelize(Array(Edge(4L,1L, "teaches"), Edge(5L,2L, "teaches"),Edge(6L, 3L, "teaches")))
     
    //Define Edges/ Relationships between Subject vs Students
    val subjectsVSstudents: RDD[Edge[String]] = sparkCtx.parallelize(
        Array(Edge(7L, 3L, "Enrolled"), Edge(8L, 3L, "Enrolled"),Edge(9L, 2L,  "Enrolled"),Edge(10L, 2L, "Enrolled"),Edge(11L, 1L, "Enrolled"),
            Edge(12L, 1L, "Enrolled"))) 
    //Join all Edges and create 1 Edge  
    val edges = subjectsVSteachers.union(subjectsVSstudents)
    //Define Object of Graph
    val graph = Graph(vertices, edges)

    //Print total number of Vertices and Edges
    println("Total vertices = " + graph.vertices.count()+", Total Edges = "+graph.edges.count())
    
    // Print Students and Teachers associated with Subject = Math
    println("Students and Teachers associated with Math....")
    //EdgeTriplet represents an edge along with the vertex attributes of its neighboring vertices.triplets
    graph.triplets.filter(f=>f.dstAttr.equalsIgnoreCase("Math")).collect().foreach(println)
    
    sparkCtx.stop()
  }

}