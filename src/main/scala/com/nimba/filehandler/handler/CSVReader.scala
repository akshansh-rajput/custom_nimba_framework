package com.nimba.filehandler.handler

import com.nimba.filehandler.handlerInterface.IReader
import scala.collection.mutable.ListBuffer


class CSVReader extends IReader
{

    def reader(source_paths: Seq[String], options: Map[String, String] = null): Seq[Map[String, String]] = {
        val data = ListBuffer[Map[String, String]]()
        // Pick files one by one
        for(source_path <- source_paths) {
            val bufferedSource = io.Source.fromFile(source_path)
            val delimiter = options.get("delimiter").get
            var header: Array[String] = null
            // looping through rows
            for (line <- bufferedSource.getLines) {
                if(header == null || header.isEmpty){
                    // extracting headers
                    header = line.split(delimiter).map(_.trim)
                }
                else {
                    // parsing columns based on header
                    val cols = line.split(delimiter).map(_.trim)
                    val colCount = header.size
                    var row = scala.collection.mutable.Map[String, String]()
                    for(i <- 0 until colCount){
                        row(header(i)) = cols(i)
                    }
                    data += row.toMap[String, String]
                }
            }
            bufferedSource.close
        }
        data.toSeq
        
    }
}