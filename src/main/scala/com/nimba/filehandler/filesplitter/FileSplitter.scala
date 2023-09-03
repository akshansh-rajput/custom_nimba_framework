package com.nimba.filehandler.filesplitter

import com.nimba.mapreducer.constants.InternalConstants._
import java.io._
import java.nio.file.Files
import java.nio.file.Paths
import scala.collection.mutable.ListBuffer


object Filesplitter
{
    var dir = ""
    var fileNumber = 1

    /**
     * Splits a source CSV file into smaller subpart files with a specified maximum number of rows.
     *
     * @param sourcePath The path to the source CSV file.
     * @param subpart The subpart identifier for the output files.
     * @return A sequence of paths to the generated subpart files.
     */
    def splitFile(sourcePath: String, subpart:String): Seq[String] = {
        dir = f"${SPLITTED_DATA_LOCATION}/${subpart}"

        val outputFiles = ListBuffer[String]()

        Files.createDirectories(Paths.get(dir))
        val bufferedSource = io.Source.fromFile(sourcePath)
        var header = ""
        var lineNumber = 0
        var outputFile = createOutputFile(fileNumber)
        for (line <- bufferedSource.getLines) {
            if(header == null || header.isEmpty){
                // extracting headers
                header = line
                outputFile.write(header + "\n")
                outputFiles += f"$dir/split_$fileNumber.csv"
            }
            else
            {
                if(lineNumber>MAX_ROW_PER_FILE){
                    outputFile.close()
                    fileNumber += 1
                    outputFile = createOutputFile(fileNumber)
                    lineNumber = 0
                    outputFile.write(header + "\n")
                    outputFiles += f"$dir/split_$fileNumber.csv"
                }
                outputFile.write(line + "\n")
                lineNumber += 1
            }
        }
        outputFile.close()
        outputFiles
    }

    def createOutputFile(fileNumber: Int): BufferedWriter = {
        val outputPath = f"$dir/split_$fileNumber.csv"
        new java.io.BufferedWriter(new FileWriter(outputPath))
  }
}