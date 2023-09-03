package com.nimba.taskcontroller.controller

import com.nimba.mapreducer.mapper.config.MapperTransformations
import com.nimba.taskcontroller.controllerInterface.ITaskController
import com.nimba.mapreducer.builderInterface.IMapperBuilder
import com.nimba.mapreducer.builderInterface.IReducerBuilder
import com.nimba.mapreducer.builder.ReducerBuilder
import com.nimba.mapreducer.builder.MapperBuilder
import scala.collection.mutable.ListBuffer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.nimba.taskcontroller.JobInvoker._
import com.nimba.mapreducer.constants.InternalConstants._
import java.io.{File, FileWriter}
import java.nio.file.Files
import java.nio.file.Paths
import com.nimba.status.interface.IStatusService
import com.nimba.status.statusservice.StatusService
import com.nimba.filehandler.FileManager

class TaskController extends ITaskController
{
    val maxMapperCount = 4
    val maxReducerCount = 4
    var mapperCount = 0
    var reducerCount = 0
    private val statusService: IStatusService = new StatusService()

    /**
     * Create and return a new instance of the IMapperBuilder.
     *
     * This method creates and returns a new instance of the IMapperBuilder interface, typically used for configuring
     * and building a mapper for data transformation and processing.
     *
     * @return A new instance of IMapperBuilder.
     */
    def createMapperBuilder(): IMapperBuilder = {
        new MapperBuilder()
    }

    /**
     * Create and return a new instance of the IReducerBuilder.
     *
     * This method creates and returns a new instance of the IReducerBuilder interface, typically used for configuring
     * and building a reducer for data aggregation and processing.
     *
     * @return A new instance of IReducerBuilder.
     */
    def createReducerBuilder(): IReducerBuilder = {
        new ReducerBuilder()
    }

    /**
     * Generate a YAML configuration file from the provided object and write it to the specified path.
     *
     * This method is used to generate configuration yaml file for mapper and reducer. It take generic config type
     * case class parameter and parse it into YAML file.
     *
     * @param config The configuration object to be serialized to YAML.
     * @param path The path where the generated YAML configuration file should be saved.
     * @tparam T The type of the configuration object.
     */
    def yamlFileGenerator[T](config: T, path: String): Unit = {
        val yamlMapper = new ObjectMapper(new YAMLFactory())
        yamlMapper.registerModule(DefaultScalaModule)
        
        val outputFile = new File(path)
        yamlMapper.writeValue(outputFile, config)
    }

    /**
     * Create the required internal directories. These dirs will be used by reducer and mapper
     * to get config, store intrim data and status of their run.
     *
     * This method creates the necessary directories specified by the `path` parameter, including any parent directories
     * that do not exist. It ensures that the directory structure required for the path is created.
     *
     * @param path The path where the directories should be created, including any necessary parent directories.
     */
    def createReqDirs(path: String): Unit = {
        Files.createDirectories(Paths.get(path))
    }

    /**
     * Check the status of multiple jobs and handle pending, completed, and failed jobs.
     *
     * This method checks the status of multiple jobs identified by their `ids` in the specified `path`. It iterates
     * through the list of job IDs and uses the `statusService` to read the status of each job. Depending on the status,
     * it handles completed jobs by printing a message, restarts failed jobs, and returns a list of failed job IDs.
     *
     * @param path The path where status information for the jobs is located.
     * @param ids A sequence of job IDs to check.
     * @return A sequence of job IDs that failed and need to be restarted.
     */
    private def jobChecker(path: String, ids: Seq[String]): Seq[String] = {
        // read status file
        var pendingIds = ids
        val failedIds = ListBuffer[String]()
        while(pendingIds.size>0){
            val idsToRemove = ListBuffer[String]()
            pendingIds.foreach(id =>{
                val status = statusService.readStatus(id, path)
                if(status.get("status").get.equalsIgnoreCase("done")){
                    println(f"====>\t\t${id} Completed\t\t<====")
                    idsToRemove += id
                }
                else if(status.get("status").get.equalsIgnoreCase("failed")){
                    println(f"====>\t\t${id} Failed\t\t<====")
                    println(f"====>\t\t${id} task will be restarted\t\t<====")
                    failedIds += id
                }
            })
            pendingIds = pendingIds.filterNot(pid=> idsToRemove.toSeq.contains(pid))
        }
        failedIds
    }

    /**
     * Run a mapper jobs in parallel, check their status, and retry failed jobs if needed.
     *
     * This method runs mapper jobs in parallel identified by their `mapperIds`. It invokes each job using the
     * `invokeJob` method. After running the jobs, it keeps checking their status until all job passed.
     * If there are failed jobs, it retries them up to a maximum of three times.
     * If all retry attempts fail, the method stops the job execution and returns `false`.
     *
     * @param mapperIds A sequence of mapper job IDs to run.
     * @param tryNumber The current retry attempt number (default is 0).
     * @return `true` if all jobs are successfully completed; `false` if any retry attempt fails.
     */
    private def mapperRunner(mapperIds: Seq[String], tryNumber: Int = 0): Boolean = {
        mapperIds.foreach(id=>{
            invokeJob(f"${MAPPER_JOB_INVOKER} ${id}")
            println(f"====>\t\t${id} Started\t\t<====")
            })
        val failedIds = jobChecker(MAPPER_STATUS_LOCATION, mapperIds)
        if(failedIds.size>0){
            if(tryNumber>3){
                println("last atempt also failed. Stoping Job")
                return false
            }
            mapperRunner(failedIds, tryNumber + 1)
        }
        true
    }

    /**
     * Run reducer jobs in parallel, check their status, and retry failed jobs if needed.
     *
     * This method runs reducer jobs in parallel identified by their `reducerIds`. It invokes each job using the
     * `invokeJob` method and keeps checking thier status until all jobs passed.
     * If there are failed jobs, it retries them up to a maximum of three times.
     * If all retry attempts fail, the method stops the job execution and returns `false`.
     *
     * @param reducerIds A sequence of reducer job IDs to run.
     * @param tryNumber The current retry attempt number (default is 0).
     * @return `true` if all jobs are successfully completed; `false` if any retry attempt fails.
     */
    private def reducerJobRunner(reducerIds: Seq[String], tryNumber: Int = 0): Boolean = {
        reducerIds.foreach(id=>{
            invokeJob(f"${REDUCER_JOB_INVOKER} ${id}")
            println(f"====>\t\t${id} Started\t\t<====")
            })
        val failedIds = jobChecker(REDUCER_STATUS_LOCATION, reducerIds)
        if(failedIds.size>0){
            if(tryNumber>3){
                println("last atempt also failed. Stoping Job")
                return false
            }
            reducerJobRunner(failedIds, tryNumber + 1)
        }
        true
    }

    /**
     * Identify how to group files amoung mapper or reducer. It create a group for each
     * mapper or reducer. 
     * 
     *
     * @param filesSize Number of files
     * @param maxAllowed Maximum parallel jobs which can run
     * @return grouping count 
     */ 
    def getGroupingValue(fileSize: Int, maxAllowed: Int): Int = {
        var grouping = 1
        if(fileSize>maxAllowed){
            grouping = (fileSize.toDouble/maxAllowed).ceil.toInt
        }
        grouping
    }

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
    def startMapperJob(files: Seq[String], config: MapperTransformations): Boolean = {
        var grouping = getGroupingValue(files.size, maxMapperCount)
        val mappers = ListBuffer[String]()

        val groupFile = files.grouped(grouping).toList
        // creating internal mapper dir if not exist
        createReqDirs(f"${INTERNAL_LOCATION}/mapper")
        // generate yaml file for each mapper
  
        groupFile.foreach(files=>{
            val fileString = files.mkString(",")
            mapperCount +=1
            val mapperId = f"mapper_${mapperCount}"
            mappers += mapperId.toString
            val newMapperConfig = config.copy(data_source = fileString)
            yamlFileGenerator(newMapperConfig, f"${INTERNAL_LOCATION}/mapper/${mapperId}.yaml" )
        })

        val status = mapperRunner(mappers)
        if(!status)
            return false
        true
    }


    /**
     * Handle and organize left source information for reducer processing.
     *
     * This method handles and organizes left source information for reducer processing. It retrieves additional source
     * information using the provided `reducerBuilder`, manages files in the `internalLocation`, and maps them by partition.
     * It combines common partition information with remaining information and distributes the files evenly among partitions.
     * The result is a map of partition folders mapped to their corresponding data files.
     *
     * @param reducerBuilder An instance of the IReducerBuilder interface to retrieve left source mapper information.
     * @param partitionFolders A sequence of partition folders used for organizing the data.
     * @param groupFile A sequence of sequences containing group files.
     * @return A map of partition folders mapped to their corresponding data files.
     */
    def handleLeftSourceInformation(reducerBuilder: IReducerBuilder, partitionFolders: Seq[String], groupFile: Seq[Seq[String]]): Map[String,String] = {
        val otherSource = reducerBuilder.getLeftSourceMapperInformation()
        val internalLocation = otherSource.getMapperConfig().intrim_loc
        val files = FileManager.manageFiles(internalLocation, "json", "joinData")
        val otherFilesMapByPartition = FileManager.reducerFileMapping(files)
        val commonPartitionMap = otherFilesMapByPartition.filterKeys(partitionFolders.contains)
        val remainingInformation = otherFilesMapByPartition.filterKeys(key => !partitionFolders.contains(key))
        var finalFilesMap = scala.collection.mutable.Map[String, String]() ++ commonPartitionMap
        if(remainingInformation.size > 0)
        {
            val normalDistributedFiles = scala.collection.mutable.Map[String, String]()
            val leftFiles = remainingInformation.values.toList
            val partitionCount = partitionFolders.size
            var counter = 0
            for(i <- 0 until leftFiles.size){
                val filesInPartition = finalFilesMap.getOrElse(partitionFolders(counter), "")
                if(filesInPartition.isEmpty)
                    finalFilesMap(partitionFolders(counter)) = leftFiles(i)
                else
                    finalFilesMap(partitionFolders(counter)) = f"${filesInPartition},${leftFiles(i)}"
                counter += 1
                if(counter == partitionCount)
                    counter = 0
            }
        }
        finalFilesMap.toMap[String, String]
    }

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
    def startReducerJob(files: Seq[String], reducerBuilder: IReducerBuilder): Boolean = {
        
        val filesMapByPartition = FileManager.reducerFileMapping(files)
        val partitionFolders = filesMapByPartition.keySet.toSeq

        var grouping = getGroupingValue(partitionFolders.size, maxReducerCount)
        val reducer = ListBuffer[String]()
        val groupFile = partitionFolders.grouped(grouping).toList
        // creating internal reducer dir if not exist
        createReqDirs(f"${INTERNAL_LOCATION}/reducer")

        var leftSourceMapping = Map[String,String]()
        // Handle Join Operation
        if(reducerBuilder.isJoinOperation())
            leftSourceMapping = handleLeftSourceInformation(reducerBuilder, partitionFolders, groupFile)

        // generate yaml file for each mapper
        groupFile.foreach(files=>{
            val actualFileList = files.map(fileKey => filesMapByPartition.get(fileKey).get)
            val fileString = actualFileList.mkString(",")
            reducerCount +=1
            val reducerId = f"reducer_${reducerCount}"
            reducer += reducerId.toString
            if(reducerBuilder.isJoinOperation())
            {
                val leftFileSourcesList = files.map(fileKey => leftSourceMapping.getOrElse(fileKey, ""))
                val leftSourceString = leftFileSourcesList.filter(x=> !x.isEmpty).mkString(",")
                reducerBuilder.joinLeftSource(leftSourceString)
            }
            val config = reducerBuilder.getReducerConfig()
            val newReducerConfig = config.copy(data_source = fileString)
            yamlFileGenerator(newReducerConfig, f"${INTERNAL_LOCATION}/reducer/${reducerId}.yaml" )
        })

        val status = reducerJobRunner(reducer)
        if(!status)
            return false
        true
    }
}
