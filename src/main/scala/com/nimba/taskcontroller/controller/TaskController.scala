package com.nimba.taskcontroller.controller

import com.nimba.mapreducer.mapper.config.MapperTransformations
import com.nimba.taskcontroller.controllerInterface.ITaskController
import com.nimba.mapreducer.builderInterface.IMapperBuilder
import com.nimba.mapreducer.builder.MapperBuilder
import scala.collection.mutable.ListBuffer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.nimba.taskcontroller.JobInvoker._
import com.nimba.mapreducer.constants.InternalConstants._
import java.io.{File, FileWriter}
import java.nio.file.Files
import java.nio.file.Paths
import com.nimba.status.interface.IStatusService
import com.nimba.status.statusservice.StatusService

class TaskController extends ITaskController
{
    val maxMapperCount = 4
    val intrimLocation = "mapreducer_internal/"
    var mapperCount = 0
    private val statusService: IStatusService = new StatusService()

    def createMapperBuilder(): IMapperBuilder = {
        new MapperBuilder()
    }

    def yamlFileGenerator[T](config: T, path: String): Unit = {
        val yamlMapper = new ObjectMapper(new YAMLFactory())
        yamlMapper.registerModule(DefaultScalaModule)
        
        val outputFile = new File(path)
        yamlMapper.writeValue(outputFile, config)
    }

    def createReqDirs(path: String): Unit = {
        Files.createDirectories(Paths.get(path))
    }

    private def jobChecker(path: String, ids: Seq[String]): Seq[String] = {
        // read status file
        var pendingIds = ids
        val failedIds = ListBuffer[String]()
        while(pendingIds.size>0){
            val idsToRemove = ListBuffer[String]()
            pendingIds.foreach(id =>{
                val status = statusService.readStatus(id, path)
                if(status.get("status").get.equalsIgnoreCase("done")){
                    println(f"====>${id} Completed<====")
                    idsToRemove += id
                }
                else if(status.get("status").get.equalsIgnoreCase("failed")){
                    println(f"====>${id} Failed<====")
                    println(f"====>${id} task will be restarted<====")
                    failedIds += id
                }
            })
            pendingIds = pendingIds.filterNot(pid=> idsToRemove.toSeq.contains(pid))
        }
        failedIds
    }

    private def mapperRunner(mapperIds: Seq[String], tryNumber: Int = 0): Boolean = {
        mapperIds.foreach(id=>{
            invokeJob(f"${MAPPER_JOB_INVOKER} ${id}")
            println(f"====>${id} Started<====")
            })
        val failedIds = jobChecker(MAPPER_STATUS_LOCATION, mapperIds)
        if(failedIds.size>0){
            if(tryNumber>3){
                println("last atempt also failed. Stoping Job")
                return false
            }
            mapperRunner(failedIds, tryNumber + 1)
        }
        true
    }

    def startMapperJob(files: Seq[String], config: MapperTransformations): Boolean = {
        val fileCount = files.size
        var grouping = 1
        val mappers = ListBuffer[String]()
        //  setting grouping value
        if(fileCount>maxMapperCount){
            grouping = (fileCount.toDouble/maxMapperCount).ceil.toInt
        }

        val groupFile = files.grouped(grouping).toList
        // creating internal mapper dir if not exist
        createReqDirs(f"${intrimLocation}mapper")
        // generate yaml file for each mapper
  
        groupFile.foreach(files=>{
            val fileString = files.mkString(",")
            mapperCount +=1
            val mapperId = f"mapper_${mapperCount}"
            mappers += mapperId.toString
            val newMapperConfig = config.copy(data_source = fileString)
            yamlFileGenerator(newMapperConfig, f"${intrimLocation}mapper/${mapperId}.yaml" )
        })

        val status = mapperRunner(mappers)
        if(!status)
            return false
        true
    }
}
