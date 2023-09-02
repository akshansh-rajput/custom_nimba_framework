package com.nimba.taskcontroller.controllerInterface

import com.nimba.mapreducer.builderInterface.IMapperBuilder
import com.nimba.mapreducer.mapper.config.MapperTransformations

abstract class ITaskController
{
    def createMapperBuilder(): IMapperBuilder

    def startMapperJob(files: Seq[String], config: MapperTransformations): Boolean
}