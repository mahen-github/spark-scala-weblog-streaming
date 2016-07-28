package dev.mahen.streaming.WebLogsStreaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.dstream.DStream
import java.util.regex.Pattern

/**
 * parse a lines of Log
 */
case class Line(
  ip: String,
  client: String,
  user: String,
  date: String,
  request: String,
  status_code: String,
  bytes: String,
  referrer: String,
  ua: String,
  countryName: String,
  city: String,
  area_code: Int,
  countryCode: String,
  region: String){
  
   override def toString() = ip +","+client+","+user+","+ date+","+request+","+status_code + ","+ bytes+","+referrer+","+ua+
   ","+countryName+","+city+","+area_code+","+ countryCode+","+region    
  
}

object Streaming {

  private var files = ""
  def main(args: Array[String]) {

    val directory = args(0)

    files = args(1)

    val sparkContext = new SparkContext(new SparkConf().setAppName("WebLogsStreaming"))
    val sparkStreamingContext = new StreamingContext(sparkContext, Seconds(25))
    //    val lines = sparkStreamingContext.socketTextStream("localhost", 9999)

    //    val words = lines.flatMap(_.split(" "))
    //
    //    val pairs = words.map(word => (word, 1))
    //    val wordCounts = pairs.reduceByKey(_ + _)
    //
    //    wordCounts.print()
    //    
    val fileStream = sparkStreamingContext.textFileStream(directory)

    //Tranformation
    val data = parseLine(fileStream)
    data.foreach(println(_))

    //Actions
    data.map {x => x.toString()}.saveAsTextFiles(args(1))
    sparkStreamingContext.start() // Start the computation
    sparkStreamingContext.awaitTermination() // Wait for the computation to terminate

  }

  def parseLine(fileStream: DStream[String]): DStream[Line] = {

    //Reges Pattern to parse the Logs
    val regex = """(\d+.\d+.\d+.\d+)\t+([-])\t([-])\t+(\[.+?\])\t+(\".+?\")\t+(\d+)\t+(\d+)\t+(\".?\")\t+(\".+?\")"""
    val p = Pattern.compile(regex)

    //DStream
    val dataV = fileStream.map { line =>
      val matcher = p.matcher(line)
      if (matcher.find) {
        val glValue = new GeoIPLookup(matcher.group(1))
        val glVal = glValue.apply()
        Line(matcher.group(1), 
            matcher.group(2), 
            matcher.group(3), 
            matcher.group(4), 
            matcher.group(5), 
            matcher.group(6), 
            matcher.group(7), 
            matcher.group(8), 
            matcher.group(9), 
            glVal._1, 
            glVal._2, 
            glVal._3, 
            glVal._4, 
            glVal._5)
      } else Line("", "", "", "", "", "", "", "", "", "", "", 0, "", "")
    }
    dataV
  }
}