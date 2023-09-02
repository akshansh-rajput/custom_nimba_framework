package com.nimba.filehandler.handlerInterface

/**
 * A data reader interface.
 *
 * This class defines a logic for reading file/files from different files format.
 *
 */
abstract class IReader
{
    /**
     * Reads data from one or more source paths based on the specified parameters.
     *
     * @param source_paths  A sequence of source paths from which data should be read.
     * @param options       An optional map of additional options for configuring the reading process.
     *                      Example file format, delimiter, etc.
     *
     * @return              The data read from the source paths. The data type may vary depending on the source.
     */
    def reader(source_paths: Seq[String], options: Map[String, String] = null): Any
}