package com.nimba.mapreducer.mapper.service 

import com.nimba.mapreducer.mapper.serviceInterface.IMapperService
import java.io.File
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.nimba.mapreducer.mapper.config.{MapperTransformations, Transformation}
import com.nimba.mapreducer.utils.Utils.pathBuilder
import com.nimba.filehandler.handlerInterface.IReader
import com.nimba.filehandler.handler.CSVReader
import com.nimba.filehandler.handlerInterface.IWriter
import com.nimba.filehandler.handler.JSONWriter
import com.nimba.filehandler.handler.CSVWriter
import java.nio.file.Files
import java.io.FileWriter
import java.nio.file.Paths




class MapperService extends IMapperService
{
    /**
     * Retrieves the MapperTransformations configuration from a YAML file.
     *
     * This method reads the MapperTransformations configuration from a YAML file and
     * convert it into MapperTransformations case class. 
     *
     * @param dir The directory where the configuration file is located.
     * @param worker_id The identifier of the worker.
     * @return An instance of MapperTransformations containing the configuration settings.
     */
    def retrieveMapperConfig(dir: String, worker_id: String): MapperTransformations = {
        val path = pathBuilder(dir, worker_id)
        val mapper = new ObjectMapper(new YAMLFactory)
        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(new File(path), classOf[MapperTransformations])
    }

    /**
     * Read data from one or more source files using the specified reader and options.
     *
     * This method reads data from the given source files using the specified reader, which can be either
     * a CSVReader or JSONReader. The options map allows you to provide configuration settings
     * for the reader. The "file_type" option determines the file type to be read (e.g., "csv").
     *
     * @param source_paths A sequence of file paths from which to read data.
     * @param options A map of configuration options for the reader.
     * @return A sequence of maps representing the read data.
     */
    def read_data(source_paths: Seq[String], options: Map[String, String]): Seq[Map[String, String]] = {
        val fileType = options.get("file_type").get
        var reader: IReader = null 
        if(fileType.equalsIgnoreCase("csv"))
            reader = new CSVReader()
        // TODO: add exception here
        val data = reader.reader(source_paths, options)
        data.asInstanceOf[Seq[Map[String, String]]]
    }

    /**
     * Write data to a destination location using the specified writer and options.
     *
     * This method writes data to the specified destination location using the selected writer, which can be either
     * a JSONWriter or a CSVWriter."storage_layer" tell the job where to save this data i.e. interim location of its a final output.
     * internal data is always written in json format but if mapper is directly writing it to external output location then it will
     * use csv format
     *
     * @param sinkLocation The location where the data should be written.
     * @param data The data to be written, which can be of any type.
     * @param options A map of configuration options for the writer.
     * @param isGroupedData Indicates whether the data is grouped and should be written accordingly.
     */
    def dataWriter(sinkLocation: String, data: Any, options: Map[String, String], isGroupedData: Boolean = false): Unit = {
        val storageLayer = options.get("storage_layer").get
        if(storageLayer.equalsIgnoreCase("internal")){
            val writer: IWriter = new JSONWriter()
            writer.fileWriter(data, isGroupedData, sinkLocation, options)
        }
        else{
            val writer: IWriter = new CSVWriter()
            writer.fileWriter(data, isGroupedData, sinkLocation, options)
        }

    }

}