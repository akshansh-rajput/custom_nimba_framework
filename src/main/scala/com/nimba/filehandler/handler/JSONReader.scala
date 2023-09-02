package com.nimba.filehandler.handler

import com.nimba.filehandler.handlerInterface.IReader
import scala.collection.mutable.ListBuffer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.io.File
import scala.io._

/**
 * A JSON file reader. Read and parse data from json file/files.
 *
 */
class JSONReader extends IReader
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
    def reader(source_paths: Seq[String], options: Map[String, String]): Any = {
        val data = ListBuffer[Map[String,Seq[Map[String, String]]]]()
        for(sourcePath <- source_paths){
            val mapper = new ObjectMapper()
            mapper.registerModule(DefaultScalaModule)
            val parsedData = mapper.readValue(new File(sourcePath),classOf[Map[String,Seq[Map[String,String]]]])
            data += parsedData
        }
        data.toSeq
    }
}