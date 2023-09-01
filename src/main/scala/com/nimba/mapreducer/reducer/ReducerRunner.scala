package com.nimba.mapreducer.reducer

import com.nimba.mapreducer.reducer.config.ReducerOperations
import com.nimba.mapreducer.reducer.serviceInterface.IReducerService
import com.nimba.mapreducer.reducer.service.ReducerService
import com.nimba.mapreducer.utils.Utils.option_builder


object ReducerRunner{
    private val reducerService: IReducerService = new ReducerService

    def main(args: Array[String]): Unit = {
        // Fetch Mapper config from yaml file
        val dir = args(0)
        val worker_id = args(1)
        val reducerConfig: ReducerOperations = reducerService.retrieveMapperConfig(dir, worker_id)
        
        // Read Data from intrim location
        val sourcePaths = reducerConfig.data_source.split(",").toSeq
        val options = Map[String, String]()
        val data = reducerService.readData(sourcePaths, options)
        println(data)

        // PERFORM OPERATION

        // SAVE DATA

        // UPDATE STATUS
    }
}