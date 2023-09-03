package com.nimba.filehandler.handler

import com.nimba.filehandler.handlerInterface.IWriter
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Paths


/**
 * A CSV file writer.
 * 
 * Define the logic to write data into csv files using provided parameters i.e.
 * delimiter, header etc.
 *
 */
class CSVWriter extends IWriter
{
    /**
     * Extracts headers from data.
     *
     * @param data  data.
     * @return      ordered header name.
     */
    def getHeader(data: Seq[Map[String, String]]): Map[String, String] = {
        val topRow = data(0)
        var keys = topRow.keySet.toSeq
        data.foreach(row=>{
            val rowHeader = row.keySet.toSeq
            val newHeader = rowHeader.filter(x=> !keys.contains(x))
            if(newHeader.size > 0)
                keys = keys ++ newHeader
        })
        val prefix = "H_"
        var header = scala.collection.mutable.Map[String, String]()
        val colCount = keys.size
        for(i <- 0 until colCount){
            header(prefix + i) = keys(i)
        }
        header.toMap[String, String]
    }

    /**
     * Build a single line delimiter sep header for csv file.
     *
     * @param header     ordered header name.
     * @param delimiter  seperator to be use.
     * @return           single line string.
     */
    def headerBuilder(header: Map[String, String], delimiter: String): String = {
        val count = header.size
        var headerRow = ""
        for(i <- 0 until count){
            val value = header.get("H_"+i).get
            headerRow = f"${headerRow}${value}"
            if(i<count-1)
                headerRow = f"${headerRow}${delimiter}"
            else
                headerRow = f"${headerRow}\n"
        }
        headerRow
    }
    
    /**
     * Writes data to a CSV file with a specified delimiter at the provided sink location.
     *
     * @param data         The data to be written.
     * @param sinkLocation The directory where the CSV file should be saved.
     * @param workerId     Using for file name so each mapper/reducer can write unique name file.
     * @param delimiter    The delimiter used to separate.
     */
    def outputWriter(data: Any, sinkLocation: String, workerId: String, delimiter: String): Unit = { 
        val writerData = data.asInstanceOf[Seq[Map[String, String]]]
        if(writerData.size == 0)
            return
        val header = getHeader(writerData)
        val path = sinkLocation
        val colCount = header.size
        Files.createDirectories(Paths.get(path))
        val outputFile = new FileWriter(path+"/"+workerId+".csv")
        val headerRow = headerBuilder(header, delimiter)
        outputFile.write(headerRow)
        writerData.foreach(row => {
            var csvData = ""
            for(i <- 0 until colCount){
                val value = row.getOrElse(header.get("H_"+i).get, "")
                csvData = f"${csvData}${value}"
                if(i<colCount-1)
                    csvData = f"${csvData}${delimiter}"
                else
                    csvData = f"${csvData}\n"
            }
            outputFile.append(csvData)

        })
        outputFile.close()
        
    }

    /**
     * Writes data to a file based on the specified parameters.
     *
     * @param data           The data to be written. It can be of any type.
     * @param isGroupedData  Indicates whether the data is grouped data or not.
     * @param sinkLocation   The location where the file should be written.
     * @param options        A map of additional options for configuring the writing process.
     *
     */
    def fileWriter(data: Any, isGroupedData: Boolean, sinkLocation: String, options: Map[String, String]): Unit = {
        val storingType = options.get("storage_layer").get
        if(storingType.equalsIgnoreCase("output")){
            outputWriter(data, sinkLocation, options.get("worker_id").get, options.get("delimiter").get)
        }
    }
}