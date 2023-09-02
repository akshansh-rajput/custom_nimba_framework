package com.nimba.filehandler.handler

import com.nimba.filehandler.handlerInterface.IReader
import scala.collection.mutable.ListBuffer


/**
 * A CSV file reader. It read csv data from file
 *
 */
class CSVReader extends IReader
{

    /**
     * Reads data from one or more source paths based on the specified parameters.
     *
     * @param source_paths  A sequence of source paths from which data should be read.
     * @param options       An optional map of additional options for configuring the reading process.
     *                      Example file format, delimiter, etc.
     *
     * @return              The data read from the source paths. The data type may vary depending on the source.
     */
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