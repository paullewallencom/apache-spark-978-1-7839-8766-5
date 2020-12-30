package chapter.four

import java.io.StringWriter
import java.util.regex.Matcher
import java.util.regex.Pattern
import org.json.simple._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.flume.SparkFlumeEvent
import org.apache.spark.graphx._
import scala.util.Random
import scala.collection.mutable.Set



class ScalaLogAnalyzer extends Serializable {

        
    
  /**
   * Utility method for transforming Flume Events into Sequence of Vertices and Edges  
   */
  def transformIntoGraph(eventArr: Array[SparkFlumeEvent]): Tuple2[Set[(VertexId, (String))],Set[Edge[String]]] = {
    
    println("Start Transformation........")
    //Defining mutable Sets for holding the Vertices and Edges
    val verticesSet: scala.collection.mutable.Set[(VertexId,String)] = scala.collection.mutable.Set()
    val edgesSet: scala.collection.mutable.Set[Edge[String]] = scala.collection.mutable.Set()

    //Creating Map of IP and Vertices ID, 
    //so that we create Edges to the same IP 
    var ipMap:Map[String,Long] = Map()

    //Looping over the Array of Flume Events
    for(event<-eventArr){
      //Get the Line of Log and Transform into Attribute Map
      val eventAttrMap = tansformLogData(new String(event.event.getBody().array()))
      
      //Creating new or Getting existing VertixID for IP coming from Events
      val ip_verticeID:Long = if(ipMap.contains(eventAttrMap.get("IP").get)){
        ipMap.get(eventAttrMap.get("IP").get).get
        
      }
      else{
        //Using Random function for defining Unique Vertex ID's
        val id = Random.nextLong()
        //Add to the Map
        ipMap+= (eventAttrMap.get("IP").get -> id)
        //Return the Value
        id
      }
      //Add Vertex for IP
      verticesSet+=((ip_verticeID,"IP="+eventAttrMap.get("IP")))
      //Creating Vertex for Request
      val request_verticeID = Random.nextLong()
      verticesSet+=((request_verticeID,"Request="+eventAttrMap.get("request")))
      //Creating Vertex for Date
      val date_verticeID = Random.nextLong()
      verticesSet+=((date_verticeID,"Date="+eventAttrMap.get("date")))
      //Creating Vertex for Method      
      val method_verticeID = Random.nextLong()
      verticesSet+=((method_verticeID,"Method="+eventAttrMap.get("method")))
      //Creating Vertex for Response Code      
      val respCode_verticeID = Random.nextLong()
      verticesSet+=((respCode_verticeID,"ResponseCode="+eventAttrMap.get("respCode")))
      
      //Defining Edges. All parameters are in relation to the User IP
      edgesSet.+=(Edge(ip_verticeID,request_verticeID,"Request")).+=(Edge(ip_verticeID,date_verticeID,"date"))
      edgesSet.+=(Edge(ip_verticeID,method_verticeID,"methodType")).+=(Edge(ip_verticeID,respCode_verticeID,"responseCode"))
      
    }
    println("End Transformation........")
    
    //Finally Return the Tuple of 2 Set containing Vertices and Edges
    return  (verticesSet,edgesSet)
 
  }
  
  /**
   * Transform the Apache log files and convert them into JSON Format
   */
  def tansformLogDataIntoJSON(logLine: String): String = {
    //Pattern which will extract the relevant data from Apache Access Log Files
    val LOG_ENTRY_PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+)"""
    val PATTERN = Pattern.compile(LOG_ENTRY_PATTERN)
    val matcher = PATTERN.matcher(logLine)

    //Matching the pattern for the each line of the Apache access Log file 
    if (!matcher.find()) {
      System.out.println("Cannot parse logline" + logLine)
    }
    
    //Creating the JSON Formated String from the Map
    import scala.collection.JavaConversions._
    val obj = new JSONObject(mapAsJavaMap(createDataMap(matcher)))
    val json = obj.toJSONString()
    
    println("JSON DATA new One - ", json )
    
    return json

  }
  
  
  /**
   * Transform the Apache log files and convert them into a Map of Key/Value pair
   */
  def tansformLogData(logLine: String): Map[String, String] = {
    //Pattern which will extract the relevant data from Apache Access Log Files
    val LOG_ENTRY_PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+)""";
    val PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);
    val matcher = PATTERN.matcher(logLine);

    //Matching the pattern for the each line of the Apache access Log file 
    if (!matcher.find()) {
      System.out.println("Cannot parse logline" + logLine);
    }
    //Finally create a Key/Value pair of extracted data and return to calling program
    createDataMap(matcher);

  }

  /**
   * Create a Map of the data which is extracted by applying Regular expression.
   */
  def createDataMap(m: Matcher): Map[String, String] = {

   return Map[String, String](
    ("IP" -> m.group(1)),
    ("client" -> m.group(2)),
    ("user" -> m.group(3)),
    ("date" -> m.group(4)),
    ("method" -> m.group(5)),
    ("request" -> m.group(6)),
    ("protocol" -> m.group(7)),
    ("respCode" -> m.group(8)),
    ("size" -> m.group(9))
  )

  }

  /**
   * Below method transform the given Log Data into a Sequence
   */

  def tansformLogDataIntoSeq(logLine: String): Seq[(String, String, String, String, String, String, String, String, String)] = {
    //Pattern which will extract the relevant data from Apache Access Log Files
    val LOG_ENTRY_PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+)""";
    val PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);
    val matcher = PATTERN.matcher(logLine);

    //Matching the pattern for the each line of the Apache access Log file 
    if (!matcher.find()) {
      System.out.println("Cannot parse logline" + logLine);
    }
    //Finally create a Key/Value pair of extracted data and return to calling program
    createSeq(matcher);

  }

  /**
   * Create the Seq and retunr back the results
   */
  def createSeq(m: Matcher): Seq[(String, String, String, String, String, String, String, String, String)] = {

    Seq((m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9)))

  }

}