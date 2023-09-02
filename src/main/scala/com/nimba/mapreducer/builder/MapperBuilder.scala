package com.nimba.mapreducer.builder

import com.nimba.mapreducer.builderInterface.IMapperBuilder
import com.nimba.mapreducer.mapper.config.Transformation
import com.nimba.mapreducer.mapper.config.MapperTransformations

class MapperBuilder extends IMapperBuilder
{
    private var mapperConfig: MapperTransformations = MapperTransformations(
        intrim_loc = "mapreducer_internal/intrim_data",
        status_loc = "mapreducer_internal/status/mapper"
    )

    private var transformationCount: Int = 0


    def dataSource(path: String): this.type = {
        this.mapperConfig = this.mapperConfig.copy(data_source = path)
        this
    }

    def dataFormat(format: String): this.type = {
        this.mapperConfig = this.mapperConfig.copy(data_format = format)
        this
    }

    def dataDelimiter(delimiter: String): this.type = {
        this.mapperConfig = this.mapperConfig.copy(data_delimiter = delimiter)
        this
    }

    def transformations(transformation: Transformation): this.type = {
        transformationCount += 1
        val key = f"t_${transformationCount}"
        var transformMap = Map(
            key -> transformation
        )
        if( transformationCount > 1){
            transformMap = transformMap ++ this.mapperConfig.transformations
        }
        this.mapperConfig = this.mapperConfig.copy(transformations = transformMap)
        this
    }

    def grouping(transformation: Transformation): this.type = {
        this.mapperConfig = this.mapperConfig.copy(group = transformation)
        this
    }

    def show(): Unit = {
        println(this.mapperConfig)
    }

    def getMapperConfig(): MapperTransformations = {
        mapperConfig
    }

    def updateInternalLocation(path: String): this.type = {
        this.mapperConfig = this.mapperConfig.copy(intrim_loc = path)
        this
    }
}