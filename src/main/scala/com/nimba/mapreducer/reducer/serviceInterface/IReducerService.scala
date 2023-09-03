package com.nimba.mapreducer.reducer.serviceInterface

import com.nimba.mapreducer.reducer.config.ReducerOperations

abstract class IReducerService
{
    /**
     * Retrieve ReducerOperations configuration from a YAML file.
     *
     * This method reads the ReducerOperations configuration from a YAML file and parse it into
     * ReducerOperations case class.
     *
     * @param dir The directory where the configuration file is located.
     * @param worker_id The identifier of the worker.
     * @return An instance of ReducerOperations containing the configuration settings.
     */
    def retrieveMapperConfig(dir: String, worker_id: String): ReducerOperations

    /**
     * Read data from one or more source files using the specified reader and options.
     *
     * This method reads data from the given source files using the specified reader, which can be of type JSONReader.
     * For now it only support JSON file (as Mapper only write data in json file format).
     *
     * @param source_paths A sequence of file paths from which to read data.
     * @param options A map of configuration options for the reader.
     * @return data
     */
    def readData(source_paths: Seq[String], options: Map[String, String]): Seq[Map[String,Seq[Map[String, String]]]]

    /**
     * Write data to a destination location using the specified writer, options, and output format.
     *
     * This method writes data to the specified destination location using a writer chosen based on the specified output format.
     * The supported output formats include "csv" and "json." The options map allows you to provide configuration settings
     * for the writer.
     *
     * @param sinkLocation The location where the data should be written.
     * @param data The sequence of maps containing the data to be written.
     * @param options A map of configuration options for the writer.
     * @param outputFormat The output format for writing data, such as "csv" or "json."
     */
    def dataWriter(sinkLocation: String, data: Seq[Map[String, String]], options: Map[String, String], outputFormat: String): Unit
}