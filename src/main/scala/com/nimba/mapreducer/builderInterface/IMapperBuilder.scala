package com.nimba.mapreducer.builderInterface

import com.nimba.mapreducer.mapper.config.Transformation
import com.nimba.mapreducer.mapper.config.MapperTransformations

abstract class IMapperBuilder
{

    /**
     * Configures the data source path for Mapper.
     *
     * @param path The path to the data source.
     * @return This instance with the updated data source path.
     */
    def dataSource(path: String): this.type

    /**
     * Configures the data format of source.
     *
     * @param format Source file format.
     * @return This instance with the updated data source path.
     */
    def dataFormat(format: String): this.type

    /**
     * Configures the delimiter for csv file source.
     *
     * @param delimiter delimiter value of source file.
     * @return This instance with the updated data source path.
     */
    def dataDelimiter(delimiter: String): this.type

    /**
     * Configures the transformation which are to be apply on source
     * data i.e. counting, filter etc.
     *
     * @param transformation case class containing transformation information.
     * @return This instance with the updated data source path.
     */
    def transformations(transformation: Transformation): this.type

    /**
     * Configures the join/grouping transformation which are to be apply on source
     * data i.e. grouping, join etc
     *
     * @param transformation case class containing transformation information.
     * @return This instance with the updated data source path.
     */
    def grouping(transformation: Transformation): this.type

    /**
     * Print all the current information about mapper config on standard
     * console
     * 
     * 
     */
    def show(): Unit

    /**
     * It will return the mapper config case class with current information
     * which are define using this builder
     *
     * @return case class MapperTransformation.
     */
    def getMapperConfig(): MapperTransformations

    /**
     * Configures the internal dir path in mapper config.
     * This path will be used by mapper to store intrim data and 
     * for other purpose.
     * 
     * @param path path of internal location
     * @return case class MapperTransformation.
     */
    def updateInternalLocation(path: String): this.type
   
}