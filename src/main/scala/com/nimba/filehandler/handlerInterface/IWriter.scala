package com.nimba.filehandler.handlerInterface

abstract class IWriter
{
    /**
     * Writes data to a file based on the specified parameters.
     *
     * @param data           The data to be written. It can be of any type.
     * @param isGroupedData  Indicates whether the data is grouped data or not.
     * @param sinkLocation   The location where the file should be written.
     * @param options        A map of additional options for configuring the writing process.
     *
     */
    def fileWriter(data: Any, isGroupedData: Boolean, sinkLocation: String, options: Map[String, String]): Unit
}