package com.nimba.status.statusservice

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.file.Paths
import java.io.File
import java.io.FileWriter
import java.nio.file.Files
import com.nimba.status.interface.IStatusService

class StatusService extends IStatusService
{

    def saveStatus(sinkLocation: String, id: String, status: String): Unit = {
        val result = Map("status" -> status)
        val objectMapper = new ObjectMapper()
        objectMapper.registerModule(DefaultScalaModule)
        val jsonData = objectMapper.writeValueAsString(result)
        val path = sinkLocation 
        Files.createDirectories(Paths.get(path))
        val outputFile = new FileWriter(path+ "/" + id + ".json")
        outputFile.write(jsonData)
        outputFile.close()
    }
    
    def updateStatus(id: String, path: String, status: String): Unit ={
        saveStatus(path, id, status)
    }

    def readStatus(id: String, path: String): Map[String, String] = {
        val filePath = f"${path}/${id}.json"
        val exists = Files.exists(Paths.get(filePath))
        if(!exists)
            return Map("status" -> "pending")
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(new File(filePath),classOf[Map[String,String]])
    }
}