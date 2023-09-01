package com.nimba.filehandler.handlerInterface

abstract class IWriter
{
    def fileWriter(data: Any, isGroupedData: Boolean, sinkLocation: String, options: Map[String, String]): Unit
}