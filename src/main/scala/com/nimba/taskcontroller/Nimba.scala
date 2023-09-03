package com.nimba.taskcontroller

import com.nimba.taskcontroller.controllerInterface.ITaskController
import com.nimba.taskcontroller.controller.TaskController
import com.nimba.mapreducer.builderInterface.IMapperBuilder
import com.nimba.mapreducer.builderInterface.IReducerBuilder
import com.nimba.filehandler.FileManager
import com.nimba.mapreducer.constants.InternalConstants._


object Nimba
{
    val taskController: ITaskController = new TaskController()
    var mapperCount: Int = 0
    var reducerCount: Int = 0
    var mapperMap: Map[String, IMapperBuilder] = Map[String, IMapperBuilder]()
    var reducerMap: Map[String, IReducerBuilder] = Map[String, IReducerBuilder]()

    /**
     * Create and return a new instance of the IMapperBuilder.
     *
     * This method creates and returns a new instance of the IMapperBuilder interface, typically used for configuring
     * and building a mapper for data transformation and processing.
     *
     * @return A new instance of IMapperBuilder.
     */
    def startNewMapper(): IMapperBuilder = {
        taskController.createMapperBuilder()
    }

    /**
     * Create and return a new instance of the IReducerBuilder.
     *
     * This method creates and returns a new instance of the IReducerBuilder interface, typically used for configuring
     * and building a reducer for data aggregation and processing.
     *
     * @return A new instance of IReducerBuilder.
     */
    def startNewReducer(): IReducerBuilder = {
        taskController.createReducerBuilder()
    }

    /**
     * After building Reducer config, This method is called and it will
     * Store instance information by storing it in map.
     * 
     * It store builder instance to it can be used by task controller for deciding number
     * of reducer and how to distribute load among reducer
     *
     * @param reducer reducer builder instance.
     */
    def finilizeReducer(reducer: IReducerBuilder): Unit = {
        reducerCount += 1
        val key = f"reducerConfig_${reducerCount}"
        reducerMap = reducerMap ++ Map(key -> reducer)
    }


    /**
     * After building Mapper config, This method is called and it will
     * Store instance information by storing it in map.
     * 
     * It store builder instance to it can be used by task controller for deciding number
     * of Mapper and how to distribute load among reducer
     *
     * @param mapper Mapper builder instance.
     */
    def finilizeMapper(mapper: IMapperBuilder): Unit = {
        mapperCount += 1
        val key = f"mapperConfig_${mapperCount}"
        mapper.updateInternalLocation(f"${INTRIM_DATA_LOCATION}/${key}")
        mapperMap = mapperMap ++ Map(key -> mapper)
    }

    /**
     * Start and manage the MapReduce process for mappers and reducers.
     *
     * This method coordinates the MapReduce process by iterating through the mapper and reducer configurations.
     * It starts mapper jobs for each configured mapper and, upon successful completion, proceeds to start reducer jobs.
     * The method manages files and configurations for each job using the `FileManager` and `taskController`.
     * If any job fails, the process is halted.
     */
    private def mapRedStarter(): Unit = {
        for(i <- 1 to mapperCount){
            val mapConfig = mapperMap.get(f"mapperConfig_${i}").get.getMapperConfig()
            val dataPath = mapConfig.data_source
            val format = mapConfig.data_format
            val files = FileManager.manageFiles(dataPath, format, f"mapperConfig_${i}")
            val status = taskController.startMapperJob(files, mapConfig)
            if(!status)
                return
        }
        println("====>\t\tMapper Task Completed\t\t<====")
        println("====>\t\tSTARTING REDUCER TASK\t\t<====")
        for(i <- 1 to reducerCount){
            val redBuilder = reducerMap.get(f"reducerConfig_${i}").get
            redBuilder.processDataSourceDetails()
            val path = redBuilder.getReducerConfig().data_source
            val format = redBuilder.getReducerConfig().data_format
            val files = FileManager.manageFiles(path, format, f"reducerConfig_${i}")
            val status = taskController.startReducerJob(files, redBuilder)
            if(!status)
                return
        }
    }

    /**
     * Start the process of defining and starting mapper and reducer in order
     * to process data. It also create internal dir for job and delete them after
     * job completed successfully.
     */ 
    def start(): Unit = {
        println(s"Directory '$INTERNAL_LOCATION' Created.")
        mapRedStarter()
        this.mapperMap = Map[String, IMapperBuilder]()
        this.reducerMap = Map[String, IReducerBuilder]()
        FileManager.deleteInternalDirs()
    }

}
