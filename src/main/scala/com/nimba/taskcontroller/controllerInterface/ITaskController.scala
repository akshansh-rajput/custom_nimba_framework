package com.nimba.taskcontroller.controllerInterface

import com.nimba.mapreducer.builderInterface.IMapperBuilder
import com.nimba.mapreducer.builderInterface.IReducerBuilder
import com.nimba.mapreducer.mapper.config.MapperTransformations
import com.nimba.mapreducer.reducer.config.ReducerOperations

abstract class ITaskController
{
    /**
     * Create and return a new instance of the IMapperBuilder.
     *
     * This method creates and returns a new instance of the IMapperBuilder interface, typically used for configuring
     * and building a mapper for data transformation and processing.
     *
     * @return A new instance of IMapperBuilder.
     */
    def createMapperBuilder(): IMapperBuilder

    /**
     * Create and return a new instance of the IReducerBuilder.
     *
     * This method creates and returns a new instance of the IReducerBuilder interface, typically used for configuring
     * and building a reducer for data aggregation and processing.
     *
     * @return A new instance of IReducerBuilder.
     */
    def createReducerBuilder(): IReducerBuilder

    /**
     * Start and manage mapper jobs based on input files and configuration.
     *
     * This method initiates the execution of mapper jobs based on a list of input files and a configuration object.
     * It calculates the number of mappers to be used based on the number of input files and the specified maximum mapper count.
     * The input files are grouped, and a YAML configuration file is generated for each mapper job. The mapper jobs are then
     * started using the `mapperRunner` method. If all mapper jobs complete successfully, the method returns `true`. If any
     * job fails, it returns `false`.
     *
     * @param files A sequence of input file paths to be processed by mapper jobs.
     * @param config The configuration object specifying mapper settings.
     * @return `true` if all mapper jobs complete successfully; `false` if any job fails.
     */
    def startMapperJob(files: Seq[String], config: MapperTransformations): Boolean

    /**
     * Start and manage reducer jobs based on input files and configuration.
     *
     * This method initiates the execution of reducer jobs based on a list of input files and a reducer builder.
     * It calculates the number of reducers to be used based on the number of partition folders and the specified maximum reducer count.
     * The input files are grouped by partition folders, and a YAML configuration file is generated for each reducer job.
     * If the reducer builder indicates a join operation, it handles the left source information and configuration.
     * The reducer jobs are then started using the `reducerJobRunner` method. If all reducer jobs complete successfully,
     * the method returns `true`. If any job fails, it returns `false`.
     *
     * @param files A sequence of input file paths to be processed by reducer jobs.
     * @param reducerBuilder An instance of the IReducerBuilder interface used to configure reducer settings.
     * @return `true` if all reducer jobs complete successfully; `false` if any job fails.
     */
    def startReducerJob(files: Seq[String], reducerBuilder: IReducerBuilder): Boolean
}