package com.nimba.mapreducer.reducer.service

import com.nimba.mapreducer.reducer.serviceInterface.IReducerService
import com.nimba.mapreducer.reducer.config.ReducerOperations
import com.nimba.mapreducer.utils.Utils.pathBuilder
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.io.File
import com.nimba.filehandler.handlerInterface.IReader
import com.nimba.filehandler.handler.JSONReader
import com.nimba.filehandler.handler.CSVWriter
import com.nimba.filehandler.handler.JSONWriter
import com.nimba.filehandler.handlerInterface.IWriter


class ReducerService extends IReducerService
{
    def retrieveMapperConfig(dir: String, worker_id: String): ReducerOperations = {
        val path = pathBuilder(dir, worker_id)
        val mapper = new ObjectMapper(new YAMLFactory)
        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(new File(path), classOf[ReducerOperations])
    }

    def readData(source_paths: Seq[String], options: Map[String, String]): Seq[Map[String,Seq[Map[String, String]]]] = {
        val reader: IReader = new JSONReader()
        val data = reader.reader(source_paths, options)
        data.asInstanceOf[Seq[Map[String,Seq[Map[String, String]]]]]
    }

    def dataWriter(sinkLocation: String, data: Seq[Map[String, String]], options: Map[String, String], outputFormat: String): Unit = {
        var writer: IWriter = null
        if(outputFormat.equalsIgnoreCase("csv"))
            writer = new CSVWriter()
        else
            writer = new JSONWriter()
        writer.fileWriter(data, false, sinkLocation, options)
    }
}