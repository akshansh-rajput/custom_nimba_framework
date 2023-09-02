package com.nimba

import com.nimba.taskcontroller.Nimba
import com.nimba.mapreducer.mapper.config.Transformation
import com.nimba.mapreducer.constants.MapperServiceTypes._


object MainRunner
{

    def task_1(): Unit = {
        val mapperBuilder = Nimba.startNewMapper()
        mapperBuilder
            .dataSource("data/clicks/")
            .dataFormat("csv")
            .dataDelimiter(",")
            .grouping(
                Transformation(
                    transformation_type = GROUP_DATA,
                    params = Map(
                        GROUP_ON_KEY -> "date"
                    )
                )
            )
          
        Nimba.finilizeMapper(mapperBuilder)
        Nimba.start()
            
    }

    def task_2(): Unit = {
        val mapperBuilder = Nimba.startNewMapper()
        mapperBuilder
            .dataSource("data/clicks/")
            .dataFormat("csv")
            .dataDelimiter(",")
            .grouping(
                Transformation(
                    transformation_type = GROUP_DATA,
                    params = Map(
                        GROUP_ON_KEY -> "user_id"
                    )
                )
            )
            
        val secondMapper = Nimba.startNewMapper()
        secondMapper
            .dataSource("data/users/")
            .dataFormat("csv")
            .dataDelimiter(",")
            .grouping(
                Transformation(
                    transformation_type = GROUP_DATA,
                    params = Map(
                        GROUP_ON_KEY -> "id"
                    )
                )
            )
            .transformations(
                Transformation(
                    transformation_type = FILTER_DATA,
                    params = Map(
                        COLUMN -> "country",
                        CHECK -> EQUAL_CHECK,
                        CHECK_VALUE -> "LT"
                    )
                )
            )

        Nimba.finilizeMapper(mapperBuilder)
        Nimba.finilizeMapper(secondMapper)
        Nimba.start()
            
    }
     
    // Main method
    def main(args: Array[String]): Unit =
    {
        // task_1()
        task_2()

        // import scala.sys.process._
        // println("STARTING 1")
        // "java -cp target/nimba-framework-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.nimba.mapreducer.mapper.MapperRunner mapreducer_int/mapper e1".!
        // println("Starting")
        // "java -cp target/nimba-framework-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.nimba.mapreducer.mapper.MapperRunner mapreducer_int/mapper e1".!
    }
}