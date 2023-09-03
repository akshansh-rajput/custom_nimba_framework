package com.nimba.mapreducer.builder

import com.nimba.mapreducer.builderInterface.IMapperBuilder
import com.nimba.mapreducer.mapper.config.Transformation
import com.nimba.mapreducer.mapper.config.MapperTransformations
import com.nimba.mapreducer.constants.InternalConstants._


class MapperBuilder extends IMapperBuilder
{

    /**
     * Configuration settings for the MapperTransformations case class.
     *
     * This variable holds the configuration settings for the MapperTransformations, including the locations of
     * the interim data and the mapper status.
     *
     * @param intrim_loc The location of the interim data.
     * @param status_loc The location of the mapper status.
     */
    private var mapperConfig: MapperTransformations = MapperTransformations(
        intrim_loc = INTRIM_DATA_LOCATION,
        status_loc = MAPPER_STATUS_LOCATION
    )

    private var transformationCount: Int = 0

    /**
     * Configures the data source path for Mapper.
     *
     * @param path The path to the data source.
     * @return This instance .
     */
    def dataSource(path: String): this.type = {
        this.mapperConfig = this.mapperConfig.copy(data_source = path)
        this
    }

    /**
     * Configures the data format of source.
     *
     * @param format Source file format.
     * @return This instance .
     */
    def dataFormat(format: String): this.type = {
        this.mapperConfig = this.mapperConfig.copy(data_format = format)
        this
    }

    /**
     * Configures the delimiter for csv file source.
     *
     * @param delimiter delimiter value of source file.
     * @return This instance .
     */
    def dataDelimiter(delimiter: String): this.type = {
        this.mapperConfig = this.mapperConfig.copy(data_delimiter = delimiter)
        this
    }

    /**
     * Configures the transformation which are to be apply on source
     * data i.e. counting, filter etc.
     *
     * @param transformation case class containing transformation information.
     * @return This instance .
     */
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

    /**
     * Configures the join/grouping transformation which are to be apply on source
     * data i.e. grouping, join etc
     *
     * @param transformation case class containing transformation information.
     * @return This instance .
     */
    def grouping(transformation: Transformation): this.type = {
        this.mapperConfig = this.mapperConfig.copy(group = transformation)
        this
    }

    /**
     * Print all the current information about mapper config on standard
     * console
     * 
     * 
     */
    def show(): Unit = {
        println(this.mapperConfig)
    }


     /**
     * It will return the mapper config case class with current information
     * which are define using this builder
     *
     * @return case class MapperTransformation.
     */
    def getMapperConfig(): MapperTransformations = {
        mapperConfig
    }

    /**
     * Configures the internal dir path in mapper config.
     * This path will be used by mapper to store intrim data and 
     * for other purpose.
     * 
     * @param path path of internal location
     * @return case class MapperTransformation.
     */
    def updateInternalLocation(path: String): this.type = {
        this.mapperConfig = this.mapperConfig.copy(intrim_loc = path)
        this
    }
}