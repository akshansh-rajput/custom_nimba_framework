package com.nimba.filehandler.handler

import com.nimba.filehandler.handlerInterface.IWriter
import java.io.FileWriter
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.SerializationFeature
import java.nio.file.Files
import java.nio.file.Paths


class JSONWriter extends IWriter
{

    def intrimDataWriter(data: Any, isGroupedData: Boolean, sinkLocation: String, workerId: String): Unit = {
        if(isGroupedData){
            val writerData = data.asInstanceOf[Map[String,Seq[Map[String, String]]]]
            val partition = writerData.keySet.toSeq
            partition.map(id=>{
                val partitionData: Map[String, Seq[Map[String, String]]] = writerData.filter(x=> x._1.equalsIgnoreCase(id))
                val objectMapper = new ObjectMapper()
                objectMapper.registerModule(DefaultScalaModule)
                val jsonData = objectMapper.writeValueAsString(partitionData)
                val path = sinkLocation +"/"+id+"/"
                Files.createDirectories(Paths.get(path))
                val outputFile = new FileWriter(path+workerId+".json")
                outputFile.write(jsonData)
                outputFile.close()
                
            })
        }  
        else{
            val writerData = data.asInstanceOf[Seq[Map[String, String]]]
            val objectMapper = new ObjectMapper()
            objectMapper.registerModule(DefaultScalaModule)
            val jsonData = objectMapper.writeValueAsString(writerData)
            val path = sinkLocation
            Files.createDirectories(Paths.get(path))
            val outputFile = new FileWriter(path+workerId+".json")
            outputFile.write(jsonData)
            outputFile.close()
        } 
        
    }

    

    def fileWriter(data: Any, isGroupedData: Boolean, sinkLocation: String, options: Map[String, String]): Unit = {
        val storingType = options.get("storage_layer").get
        if(storingType.equalsIgnoreCase("internal")){
            intrimDataWriter(data, isGroupedData, sinkLocation, options.get("worker_id").get)
        }
        
    }
}