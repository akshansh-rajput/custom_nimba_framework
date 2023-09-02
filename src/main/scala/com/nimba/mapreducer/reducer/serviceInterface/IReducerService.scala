package com.nimba.mapreducer.reducer.serviceInterface

import com.nimba.mapreducer.reducer.config.ReducerOperations

abstract class IReducerService
{
    def retrieveMapperConfig(dir: String, worker_id: String): ReducerOperations

    def readData(source_paths: Seq[String], options: Map[String, String]): Seq[Map[String,Seq[Map[String, String]]]]

    def dataWriter(sinkLocation: String, data: Seq[Map[String, String]], options: Map[String, String], outputFormat: String): Unit
}