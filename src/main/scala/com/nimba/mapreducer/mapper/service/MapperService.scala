package com.nimba.mapreducer.mapper.service 

import com.nimba.mapreducer.mapper.serviceInterface.IMapperService
import java.io.File
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.nimba.mapreducer.mapper.config.{MapperTransformations, Transformation}
import com.nimba.mapreducer.utils.Utils.pathBuilder
import com.nimba.filehandler.handlerInterface.IReader
import com.nimba.filehandler.handler.CSVReader
import com.nimba.filehandler.handlerInterface.IWriter
import com.nimba.filehandler.handler.JSONWriter
import com.nimba.filehandler.handler.CSVWriter
import java.nio.file.Files
import java.io.FileWriter
import java.nio.file.Paths




class MapperService extends IMapperService
{
    def retrieveMapperConfig(dir: String, worker_id: String): MapperTransformations = {
        val path = pathBuilder(dir, worker_id)
        val mapper = new ObjectMapper(new YAMLFactory)
        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(new File(path), classOf[MapperTransformations])
    }

    def read_data(source_paths: Seq[String], options: Map[String, String]): Seq[Map[String, String]] = {
        val fileType = options.get("file_type").get
        var reader: IReader = null 
        if(fileType.equalsIgnoreCase("csv"))
            reader = new CSVReader()
        // TODO: add exception here
        val data = reader.reader(source_paths, options)
        data.asInstanceOf[Seq[Map[String, String]]]
    }

    def dataWriter(sinkLocation: String, data: Any, options: Map[String, String], isGroupedData: Boolean = false): Unit = {
        val storageLayer = options.get("storage_layer").get
        if(storageLayer.equalsIgnoreCase("internal")){
            val writer: IWriter = new JSONWriter()
            writer.fileWriter(data, isGroupedData, sinkLocation, options)
        }
        else{
            val writer: IWriter = new CSVWriter()
            writer.fileWriter(data, isGroupedData, sinkLocation, options)
        }

    }

    def saveStatus(sinkLocation: String, options: Map[String, String]): Unit = {
        val result = Map("status" -> "done")
        val objectMapper = new ObjectMapper()
        objectMapper.registerModule(DefaultScalaModule)
        val jsonData = objectMapper.writeValueAsString(result)
        val id = options.get("worker_id").get
        val path = sinkLocation 
        Files.createDirectories(Paths.get(path))
        val outputFile = new FileWriter(path+ "/" + id + ".json")
        outputFile.write(jsonData)
        outputFile.close()
    }

    def updateStatus(id: String, path: String): Unit = {
        val options = Map(
            "worker_id" -> id
        )
        saveStatus(path,options)
    }

}