package com.nimba.taskcontroller

import com.nimba.taskcontroller.controllerInterface.ITaskController
import com.nimba.taskcontroller.controller.TaskController
import com.nimba.mapreducer.builderInterface.IMapperBuilder
import com.nimba.filehandler.FileManager


object Nimba
{
    val taskController: ITaskController = new TaskController()
    var mapperCount: Int = 0
    var mapperMap: Map[String, IMapperBuilder] = Map[String, IMapperBuilder]()

    def startNewMapper(): IMapperBuilder = {
        taskController.createMapperBuilder()
    }



    def finilizeMapper(mapper: IMapperBuilder): Unit = {
        mapperCount += 1
        val key = f"mapperConfig_${mapperCount}"
        mapper.updateInternalLocation(f"mapreducer_internal/intrim_data/${key}")
        mapperMap = mapperMap ++ Map(key -> mapper)
    }

    private def mapRedStarter(): Unit = {
        for(i <- 1 to mapperCount){
            val mapConfig = mapperMap.get(f"mapperConfig_${i}").get.getMapperConfig()
            val dataPath = mapConfig.data_source
            val format = mapConfig.data_format
            val files = FileManager.manageFiles(dataPath, format)
            val status = taskController.startMapperJob(files, mapConfig)
            if(!status)
                return
        }
    }

    def start(): Unit = {
        mapRedStarter()
        // this.mapperMap = Map[String, IMapperBuilder]()
    }

}
