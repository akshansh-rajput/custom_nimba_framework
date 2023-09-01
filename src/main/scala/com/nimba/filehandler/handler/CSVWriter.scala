package com.nimba.filehandler.handler

import com.nimba.filehandler.handlerInterface.IWriter
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Paths


class CSVWriter extends IWriter
{
    def getHeader(data: Seq[Map[String, String]]): Map[String, String] = {
        val topRow = data(0)
        val keys = topRow.keySet.toSeq
        val prefix = "H_"
        var header = scala.collection.mutable.Map[String, String]()
        val colCount = keys.size
        for(i <- 0 until colCount){
            header(prefix + i) = keys(i)
        }
        header.toMap[String, String]
    }

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
    

    def outputWriter(data: Any, sinkLocation: String, workerId: String, delimiter: String): Unit = { 
        val writerData = data.asInstanceOf[Seq[Map[String, String]]]
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
                val value = row.get(header.get("H_"+i).get).get
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


    def fileWriter(data: Any, isGroupedData: Boolean, sinkLocation: String, options: Map[String, String]): Unit = {
        val storingType = options.get("storage_layer").get
        if(storingType.equalsIgnoreCase("output")){
            outputWriter(data, sinkLocation, options.get("worker_id").get, options.get("delimiter").get)
        }
    }
}