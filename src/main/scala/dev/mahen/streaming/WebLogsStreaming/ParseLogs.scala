package dev.mahen.streaming.WebLogsStreaming

import org.apache.commons.io.IOUtils

class ParseLogs(val line: String) {

  def parse() {

    val logFileStream = this.getClass.getClassLoader().getResourceAsStream("logs/sample_iis.log");

    val lines = scala.io.Source.fromInputStream(logFileStream).getLines()
    
 

    while (lines.hasNext) {
      println(lines.next())
    }

  }

}