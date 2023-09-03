package com.nimba.filehandler

import java.io.File
import java.nio.file.{Files, Paths}
import scala.collection.mutable.ListBuffer
import com.nimba.mapreducer.constants.InternalConstants._
import java.io.IOException
import com.nimba.filehandler.filesplitter.Filesplitter

/**
 * File Utils for Task controller. Task controller use this file manager
 * to perform various operation related to file i.e. splitting files into small files,
 * check and list down files present in directory for processing.
 * 
 */
object FileManager
{
    /**
     * Get list of files present inside given directory.
     * 
     * @param path  Directory path
     * 
     * @return      List of files present inside directory   
     */
    def listFilesInDir(path: String): Seq[String] = {
        val dir = Paths.get(path)
        val files = Files.list(dir)
        val fileList = ListBuffer[String]()
        files.forEach{fPath=>
            if(Files.isRegularFile(fPath))
                fileList += Paths.get(path,fPath.getFileName.toString()).toString
            else
            {
                val innerFiles = listFilesInDir(Paths.get(path,fPath.getFileName.toString()).toString)
                fileList ++= innerFiles
            }
        }
        files.close()
        fileList
    }

    /**
     * Filters a sequence of file paths to include only those with a specified file extension.
     *
     * @param files       A sequence of file paths to be filtered.
     * @param extension   The file extension to filter by (e.g., ".json", ".csv").
     * @return            A new sequence containing only the file paths that match the specified file extension.
     */
    def filterFiles(files: Seq[String], extension: String): Seq[String] = {
        files.filter(x=> x.endsWith(extension))
    }

    // def needToSplit(files: String): Boolean

    /**
     * Checks if the specified path is a file or not.
     *
     *
     * @param path    The path to be checked.
     * @return        true if the path is a file, false otherwise.
     */
    def isFile(path: String): Boolean = {
        val file = new File(path)
        file.isFile()
    }

    /**
     * central point to perform various operation related to files i.e. splitting files,
     * filtering files, listing files etc.
     *
     * @param path       The path of a file or directory.
     * @param extension  The file extension to use for filtering files (e.g., ".json", ".csv").
     * @param subpart    Dir name used by splitter function to store splitted files.
     * @return           A sequence of file paths matching the specified extension if the path represents a directory,
     */
    def manageFiles(path: String, extension: String, subpart: String): Seq[String] = {
        val isOnlyFile = isFile(path)
        if(isOnlyFile){
            return Filesplitter.splitFile(path, subpart)
        }else{
            val fileLists = listFilesInDir(path)
            val reqFiles = filterFiles(fileLists, extension)
            return reqFiles
        }
    }


    /**
     * Helper File method. Create a map from list of file base of partition
     * folder name. It help to assign same key data to same reducer
     * 
     * @param files     List of internal files created by mapper.
     * @return          Mapped file list based on partition folder.
     */
    def reducerFileMapping(files: Seq[String]): Map[String, String] = {
        
        var fileMap = scala.collection.mutable.Map[String, String]()
        files.foreach(file => {
            val filePathParts = file.split("/")
            val partitionFolder = filePathParts(filePathParts.size - 2)
            val exisitngFiles = fileMap.getOrElse(partitionFolder, "")
            if(exisitngFiles.isEmpty)
                fileMap(partitionFolder) = file
            else
                fileMap(partitionFolder) = f"${exisitngFiles},${file}"
        })
        fileMap.toMap[String, String]

    }


    /**
     * Delete Files and dirs 
     * 
     * @param directory     Dir path
     * 
     */ 
    def deleteDirectory(directory: File): Unit = {
        if (directory.isDirectory) {
            val files = directory.listFiles()
            if (files != null) {
                for (file <- files) {
                    deleteDirectory(file)
                }
            }
        }
        if (!directory.delete()) {
            throw new IOException(s"Failed to delete directory: ${directory.getAbsolutePath}")
        }
    }

    /**
     * Delete Internal dirs and files created by this job
     * 
     */ 
    def deleteInternalDirs():Unit = {
        val directory = new File(INTERNAL_LOCATION)
        if (directory.exists()) {
            try {
                deleteDirectory(directory)
                println(s"Directory '$INTERNAL_LOCATION' deleted.")
            } catch {
                case e: IOException =>
                println(s"Error deleting directory: ${e.getMessage}")
            }
        } 
    }
}