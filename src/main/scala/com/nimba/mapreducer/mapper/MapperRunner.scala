package com.nimba.mapreducer.mapper

import com.nimba.mapreducer.mapper.service.MapperService
import com.nimba.mapreducer.mapper.serviceInterface.IMapperService
import com.nimba.mapreducer.utils.Utils.option_builder
import com.nimba.mapreducer.mapper.config.MapperTransformations
import com.nimba.mapreducer.mapper.serviceInterface.IProcessTransformations
import com.nimba.mapreducer.mapper.service.ProcessTransformations
import com.nimba.status.interface.IStatusService
import com.nimba.status.statusservice.StatusService
import com.nimba.mapreducer.exception.NimbaMapRedException


object MapperRunner{

    private val mapperService: IMapperService = new MapperService()
    private val transformationService: IProcessTransformations = new ProcessTransformations()
    private val statusService: IStatusService = new StatusService()

    var status = "done"
    var exception: Exception = null
    def main(args: Array[String]): Unit = {
        // Fetch Mapper config from yaml file
        val dir = args(0)
        val worker_id = args(1)
        val mapperConfig: MapperTransformations = mapperService.retrieveMapperConfig(dir, worker_id)

        // Read Data from source
        try {
            val source_path: Seq[String] = mapperConfig.data_source.split(",").toSeq
            val options = option_builder(mapperConfig)
            val data = mapperService.read_data(source_path, options)

            // Apply Transformation in sequantial order and saving in file storage system
            val newData = transformationService.applyTransformations(mapperConfig.transformations, data)
            if(mapperConfig.group != null){
                val groupedData = transformationService.applyGrouping(mapperConfig.group, newData)
                val writerOptions = Map(
                    "worker_id" -> worker_id,
                    "storage_layer" -> "internal"
                )
                mapperService.dataWriter(mapperConfig.intrim_loc, groupedData, writerOptions, true)
            }
            else{
                val writerOptions = Map(
                    "worker_id" -> worker_id,
                    "storage_layer" -> "output",
                    "delimiter" -> mapperConfig.data_delimiter
                )
                mapperService.dataWriter(mapperConfig.output_loc, newData, writerOptions)
            }
        } catch {
            case ex: Exception => 
                exception = ex
                status = "failed"
        }
        
        
        // Update the status of worker
        statusService.updateStatus(worker_id, mapperConfig.status_loc, status)
        if(exception != null)
            throw new NimbaMapRedException(f"Error occure while running mapper. ${exception.getMessage}")
    }
}