package com.nimba.mapreducer.utils

import com.nimba.mapreducer.mapper.config.MapperTransformations

object Utils{
    def pathBuilder(dir: String, worker_id: String): String = {
        f"$dir/$worker_id.yaml"
    }

    def option_builder(config: MapperTransformations): Map[String, String] = {
        val file_type = config.data_format
        if(file_type.equalsIgnoreCase("csv")){
            Map(
                "file_type" -> file_type,
                "delimiter" -> config.data_delimiter,
                "include_header" -> config.include_header,
                "table" -> config.table
            )
        }
        else{
            Map(
                "file_type" -> file_type,
                "table" -> config.table
            )
        }
    }

}