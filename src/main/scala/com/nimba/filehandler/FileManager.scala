package com.nimba.filehandler

import java.io.File
import java.nio.file.{Files, Paths}
import scala.collection.mutable.ListBuffer

object FileManager
{
    def listFilesInDir(path: String): Seq[String] = {
        val dir = Paths.get(path)
        val files = Files.list(dir)
        val fileList = ListBuffer[String]()
        files.forEach{fPath=>
            if(Files.isRegularFile(fPath))
                fileList += Paths.get(path,fPath.getFileName.toString()).toString
        }
        files.close()
        fileList
    }

    def filterFiles(files: Seq[String], extension: String): Seq[String] = {
        files.filter(x=> x.endsWith(extension))
    }

    // def needToSplit(files: String): Boolean

    def isFile(path: String): Boolean = {
        val file = new File(path)
        file.isFile()
    }

    def manageFiles(path: String, extension: String): Seq[String] = {
        val fileOrDir = isFile(path)
        if(fileOrDir){

        }else{
            val fileLists = listFilesInDir(path)
            val reqFiles = filterFiles(fileLists, extension)
            return reqFiles
        }
        Seq("a")
    }
}