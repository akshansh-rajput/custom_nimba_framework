package com.nimba.mapreducer.mapper.serviceInterface

import com.nimba.mapreducer.mapper.config.MapperTransformations


abstract class IMapperService
{
    def retrieveMapperConfig(dir: String, worker_id: String): MapperTransformations

    def read_data(source_paths: Seq[String], options: Map[String, String]): Seq[Map[String, String]]

    def dataWriter(sinkLocation: String, data: Any, options: Map[String, String], isGroupedData: Boolean = false): Unit

    def updateStatus(id: String, path: String): Unit
}