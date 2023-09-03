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

    /**
     * Save status information of mapper and reducer to a JSON file at the specified location.
     *
     * This method creates a JSON file of the status information at the specified
     * sinkLocation.
     *
     * @param sinkLocation The directory where the JSON file should be saved.
     * @param id The identifier used as the filename (without the ".json" extension).
     * @param status The status information to be saved.
     */
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
    
    /**
     * Save status information of mapper and reducer to a JSON file at the specified location.
     *
     * This method creates a JSON file of the status information at the specified
     * sinkLocation.
     *
     * @param id The identifier used as the filename (without the ".json" extension).
     * @param path The directory where the JSON file should be saved.
     * @param status The status information to be saved.
     */
    def updateStatus(id: String, path: String, status: String): Unit ={
        saveStatus(path, id, status)
    }

    /**
     * Read status information of mapper/reducer from a JSON file with the specified identifier.
     *
     * This method reads status information from a JSON file located at the specified path, where the filename is based
     * on the provided id. If the file exists, it is deserialized into a Map containing status information.
     * If the file does not exist, a default status of "pending" is returned.
     *
     * @param id The identifier used to construct the filename (without the ".json" extension).
     * @param path The directory where the JSON file is expected to be located.
     * @return A Map containing the status information or a default "pending" status if the file does not exist.
     */
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