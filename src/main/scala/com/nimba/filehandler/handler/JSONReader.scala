package com.nimba.filehandler.handler

import com.nimba.filehandler.handlerInterface.IReader
import scala.collection.mutable.ListBuffer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.io.File
import scala.io._

class JSONReader extends IReader
{
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