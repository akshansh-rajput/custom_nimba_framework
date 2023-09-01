package com.nimba.filehandler.handlerInterface

abstract class IReader
{
    def reader(source_paths: Seq[String], options: Map[String, String] = null): Any
}