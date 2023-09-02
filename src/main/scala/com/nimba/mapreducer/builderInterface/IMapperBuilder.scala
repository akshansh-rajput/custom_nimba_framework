package com.nimba.mapreducer.builderInterface

import com.nimba.mapreducer.mapper.config.Transformation
import com.nimba.mapreducer.mapper.config.MapperTransformations

abstract class IMapperBuilder
{

    def dataSource(path: String): this.type

    def dataFormat(format: String): this.type

    def dataDelimiter(delimiter: String): this.type

    def transformations(transformation: Transformation): this.type

    def grouping(transformation: Transformation): this.type

    def show(): Unit

    def getMapperConfig(): MapperTransformations

    def updateInternalLocation(path: String): this.type
   
}