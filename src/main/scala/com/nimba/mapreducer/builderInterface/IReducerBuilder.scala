package com.nimba.mapreducer.builderInterface

import com.nimba.mapreducer.reducer.config.Operation
import com.nimba.mapreducer.reducer.config.ReducerOperations


abstract class IReducerBuilder
{

    /**
     * Configures the data format for the final output file.
     *
     * @param format Source file format.
     * @return This instance with the output data path.
     */
    def outputFormat(format: String): this.type

    /**
     * Configures the location where to save the final data.
     *
     * @param path The path to save the final data.
     * @return This instance.
     */
    def outputLocation(path: String): this.type

    /**
     * Configures the operation which reducer needs to perform on data
     *
     * @param operation case class containing operation information.
     * @return This instance .
     */
    def transformations(operation: Operation): this.type

    /**
     * Display the information on standard output console.
     * 
     */ 
    def show(): Unit

    /**
     * It will return the reducer config case class with current information
     * which are define using this builder
     *
     * @return case class ReducerOperations.
     */
    def getReducerConfig(): ReducerOperations

    /**
     * Configures the other data source which will be use for joining.
     *
     * @param paths Other data source path.
     * @return This instance .
     */
    def joinLeftSource(paths: String): this.type

    /**
     * Configures the join operation information i.e. defining the left data reference,
     * nature of join.
     *
     * @param leftData Other data reference.
     * @param joinType nature of join
     * @return This instance .
     */
    def join(leftData: IMapperBuilder, joinType: String ): this.type

    /**
     * Configures the main data reference. Connect information of mapper to reducer.
     * Reducer can see all the information define under mapper, so identify source files.
     *
     * @param mainData mapper data reference
     * @return This instance .
     */
    def data(mainData: IMapperBuilder): this.type

    /**
     * Update the source data format and source using main data mapper config
     *
     * @return This instance .
     */
    def processDataSourceDetails(): this.type

    /**
     * Information whether this reducer will perform join operation or not
     *
     * @return True is it is join operation otherwise false .
     */
    def isJoinOperation(): Boolean

    def getLeftSourceMapperInformation(): IMapperBuilder

    /**
     * Configures the delimiter for final output csv file.
     *
     * @param delimiter delimiter value of output file.
     * @return This instance with the updated data delimiter.
     */
    def outputDelimiter(delimiter: String): this.type
   
}