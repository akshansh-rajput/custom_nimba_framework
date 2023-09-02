package com.nimba.filehandler.handler

import com.nimba.filehandler.handlerInterface.IWriter
import java.io.FileWriter
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.SerializationFeature
import java.nio.file.Files
import java.nio.file.Paths


/**
 * A JSON file writer. It write data into json file after converting it to
 * json string. for multiple json object it write in single line in array of 
 * json object
 *
 */
class JSONWriter extends IWriter
{

    /**
     * Writes data to a json file. If data is grouped then it will write data in partition folder, partition by id.
     *
     * @param data           The data to be written. It can be of any type.
     * @param isGroupedData  Indicates whether the data is grouped data or not.
     * @param sinkLocation   The location where the file should be written.
     * @param workerId       Using for file name so each mapper/reducer can write unique name file.
     *
     */
    def dataWriter(data: Any, isGroupedData: Boolean, sinkLocation: String, workerId: String): Unit = {
        if(isGroupedData){
            val writerData = data.asInstanceOf[Map[String,Seq[Map[String, String]]]]
            val partition = writerData.keySet.toSeq
            partition.map(id=>{
                val partitionData: Map[String, Seq[Map[String, String]]] = writerData.filter(x=> x._1.equalsIgnoreCase(id))
                val objectMapper = new ObjectMapper()
                objectMapper.registerModule(DefaultScalaModule)
                val jsonData = objectMapper.writeValueAsString(partitionData)
                val path = sinkLocation +"/"+id
                Files.createDirectories(Paths.get(path))
                val outputFile = new FileWriter(path+"/"+workerId+".json")
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
            val outputFile = new FileWriter(path+"/"+workerId+".json")
            outputFile.write(jsonData)
            outputFile.close()
        } 
        
    }

    
    /**
     * Writes data to a file based on the specified parameters.
     *
     * @param data           The data to be written. It can be of any type.
     * @param isGroupedData  Indicates whether the data is grouped data or not.
     * @param sinkLocation   The location where the file should be written.
     * @param options        A map of additional options for configuring the writing process.
     *
     */
    def fileWriter(data: Any, isGroupedData: Boolean, sinkLocation: String, options: Map[String, String]): Unit = {
        dataWriter(data, isGroupedData, sinkLocation, options.get("worker_id").get)
        
    }
}