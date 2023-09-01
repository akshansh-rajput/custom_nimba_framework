package com.nimba.mapreducer.reducer

import com.nimba.mapreducer.reducer.config.ReducerOperations
import com.nimba.mapreducer.reducer.serviceInterface.IReducerService
import com.nimba.mapreducer.reducer.service.ReducerService
import com.nimba.mapreducer.reducer.serviceInterface.IReducerOperation
import com.nimba.mapreducer.reducer.service.ReducerOperation
import com.nimba.mapreducer.utils.Utils.option_builder


object ReducerRunner{
    private val reducerService: IReducerService = new ReducerService()
    private val reducerOperations: IReducerOperation = new ReducerOperation()

    def main(args: Array[String]): Unit = {
        // Fetch Mapper config from yaml file
        val dir = args(0)
        val workerId = args(1)
        val reducerConfig: ReducerOperations = reducerService.retrieveMapperConfig(dir, workerId)
        
        // Read Data from intrim location
        val sourcePaths = reducerConfig.data_source.split(",").toSeq
        val options = Map[String, String]()
        val data = reducerService.readData(sourcePaths, options)

        // Performing reducer operation on data
        val result = reducerOperations.applyOperations(reducerConfig.transformations, data)

        // Saving final result in csv file format
        val writerOptions = Map(
            "storage_layer" -> "output",
            "worker_id" -> workerId,
            "delimiter" -> reducerConfig.data_delimiter
        )
        reducerService.dataWriter(reducerConfig.output_loc,result, writerOptions, reducerConfig.output_format)

        // UPDATE STATUS
    }
}