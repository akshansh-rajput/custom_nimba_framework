package com.nimba.mapreducer.builder

import com.nimba.mapreducer.builderInterface.IReducerBuilder
import com.nimba.mapreducer.reducer.config.ReducerOperations
import com.nimba.mapreducer.reducer.config.Operation
import com.nimba.mapreducer.builderInterface.IMapperBuilder
import com.nimba.mapreducer.constants.ReducerServiceTypes._
import com.nimba.mapreducer.constants.InternalConstants._

class ReducerBuilder extends IReducerBuilder
{

    private var reducerConfig: ReducerOperations = ReducerOperations(
        status_loc = REDUCER_STATUS_LOCATION
    )
    private var isJoin: Boolean = false
    private var leftData: IMapperBuilder = _ 
    private var mainData: IMapperBuilder = _
    private var joinType: String = INNER_JOIN

    /**
     * Configures the data source path for reducer.
     *
     * @param path The path to the data source.
     * @return This instance with the updated data source path.
     */
    private def dataSource(path: String): this.type = {
        this.reducerConfig = this.reducerConfig.copy(data_source = path)
        this
    }

    /**
     * Configures the data format of source.
     *
     * @param format Source file format.
     * @return This instance with the updated data format.
     */
    private def dataFormat(format: String): this.type = {
        this.reducerConfig = this.reducerConfig.copy(data_format = format)
        this
    }

    /**
     * Configures the delimiter for csv file source.
     *
     * @param delimiter delimiter value of source file.
     * @return This instance with the updated data delimiter.
     */
    private def dataDelimiter(delimiter: String): this.type = {
        this.reducerConfig = this.reducerConfig.copy(data_delimiter = delimiter)
        this
    }

    /**
     * Configures the data format for the final output file.
     *
     * @param format Source file format.
     * @return This instance with the output data path.
     */
    def outputFormat(format: String): this.type = {
        this.reducerConfig = this.reducerConfig.copy(output_format = format)
        this
    }

    /**
     * Configures the delimiter for final output csv file.
     *
     * @param delimiter delimiter value of output file.
     * @return This instance with the updated data delimiter.
     */
    def outputDelimiter(delimiter: String): this.type = {
        this.reducerConfig = this.reducerConfig.copy(output_delimiter = delimiter)
        this
    }

    /**
     * Configures the location where to save the final data.
     *
     * @param path The path to save the final data.
     * @return This instance.
     */
    def outputLocation(path: String): this.type = {
        this.reducerConfig = this.reducerConfig.copy(output_loc = path)
        this
    }

    /**
     * Configures the operation which reducer needs to perform on data
     *
     * @param operation case class containing operation information.
     * @return This instance .
     */
    def transformations(operation: Operation): this.type = {
        this.reducerConfig = this.reducerConfig.copy(transformations = operation)
        this
    }

    /**
     * Display the information on standard output console.
     * 
     */ 
    def show(): Unit = {
        println(this.reducerConfig)
    }

    /**
     * It will return the reducer config case class with current information
     * which are define using this builder
     *
     * @return case class ReducerOperations.
     */
    def getReducerConfig(): ReducerOperations = {
        reducerConfig
    }

    /**
     * Configures the other data source which will be use for joining.
     *
     * @param paths Other data source path.
     * @return This instance .
     */
    def joinLeftSource(paths: String): this.type = {
        val opers = Operation(
            transformation_type = JOIN_OPERATION,
            params = Map(
                JOIN_TYPE_KEY        -> this.joinType,
                LEFT_SOURCE_PATH_KEY -> paths
            )
        )
        this.reducerConfig = this.reducerConfig.copy(transformations = opers)
        this
    }

    /**
     * Configures the join operation information i.e. defining the left data reference,
     * nature of join.
     *
     * @param leftData Other data reference.
     * @param joinType nature of join
     * @return This instance .
     */
    def join(leftData: IMapperBuilder, joinType: String ): this.type = {
        this.isJoin = true
        this.joinType = joinType
        this.leftData = leftData
        this
    }

    /**
     * Configures the main data reference. Connect information of mapper to reducer.
     * Reducer can see all the information define under mapper, so identify source files.
     *
     * @param mainData mapper data reference
     * @return This instance .
     */
    def data(mainData: IMapperBuilder): this.type = {
        this.mainData = mainData
        this
    }

    /**
     * Update the source data format and source using main data mapper config
     *
     * @return This instance .
     */
    def processDataSourceDetails(): this.type = {
        val mapperConfig = mainData.getMapperConfig()
        val internalLocation = mapperConfig.intrim_loc
        this.dataFormat("json")
        this.dataSource(internalLocation)
        this
    }

    /**
     * Information whether this reducer will perform join operation or not
     *
     * @return True is it is join operation otherwise false .
     */
    def isJoinOperation(): Boolean = {
        this.isJoin
    }

    def getLeftSourceMapperInformation(): IMapperBuilder = {
        this.leftData
    }
}