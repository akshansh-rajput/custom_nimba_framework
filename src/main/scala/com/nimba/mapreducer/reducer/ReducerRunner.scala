package com.nimba.mapreducer.reducer

import com.nimba.mapreducer.reducer.config.ReducerOperations
import com.nimba.mapreducer.reducer.serviceInterface.IReducerService
import com.nimba.mapreducer.reducer.service.ReducerService
import com.nimba.mapreducer.reducer.serviceInterface.IReducerOperation
import com.nimba.mapreducer.reducer.service.ReducerOperation
import com.nimba.mapreducer.utils.Utils.option_builder
import com.nimba.status.interface.IStatusService
import com.nimba.status.statusservice.StatusService
import com.nimba.mapreducer.constants.InternalConstants._
import com.nimba.mapreducer.constants.ReducerServiceTypes._
import com.nimba.mapreducer.exception.NimbaMapRedException


object ReducerRunner{
    private val reducerService: IReducerService = new ReducerService()
    private val reducerOperations: IReducerOperation = new ReducerOperation()
    private val statusService: IStatusService = new StatusService()
    var status = "done"
    var exception: Exception = null

    def isJoinNeeded(operationType: String): Boolean = {
        operationType.equalsIgnoreCase(JOIN_OPERATION)
    }

    def main(args: Array[String]): Unit = {
        // Fetch Mapper config from yaml file
        val dir = args(0)
        val workerId = args(1)
        val reducerConfig: ReducerOperations = reducerService.retrieveMapperConfig(dir, workerId)
        
        try {
            // Read Data from intrim location
            val sourcePaths = reducerConfig.data_source.split(",").toSeq
            val options = Map[String, String]()
            val data = reducerService.readData(sourcePaths, options)

            // READ LEFT SOURCE DATA IF NEEDED
            var leftData =  Seq[Map[String, Seq[Map[String, String]]]]()
            var isJoinOperation = isJoinNeeded(reducerConfig.transformations.transformation_type)
            var joinType = ""
            if(isJoinOperation){
                val params = reducerConfig.transformations.params
                val leftSource = params.get(LEFT_SOURCE_KEY).get
                joinType = params.get(JOIN_TYPE_KEY).get
                if(!leftSource.isEmpty)
                    leftData = reducerService.readData(leftSource.split(","), options)
            }

            // Performing reducer operation on data
            var result = Seq[Map[String, String]]()
            if(isJoinOperation)
                result = reducerOperations.join(data, leftData, joinType)
            else
                result = reducerOperations.applyOperations(reducerConfig.transformations, data)

            // Saving final result in csv file format
            val writerOptions = Map(
                STORAGE_LAYER -> OUTPUT_STORAGE_LAYER,
                WORKER_ID -> workerId,
                DELIMITER -> reducerConfig.data_delimiter
            )
            reducerService.dataWriter(reducerConfig.output_loc,result, writerOptions, reducerConfig.output_format)
        } catch {
            case ex: Exception => 
                exception = ex
                status = "failed" 
        }

        // Update the status of worker
        statusService.updateStatus(workerId, reducerConfig.status_loc, status)
        if(exception != null)
            throw new NimbaMapRedException(f"Error occure while running reducer. ${exception.getMessage}")
    }
}