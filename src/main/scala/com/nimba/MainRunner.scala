package com.nimba

import com.nimba.taskcontroller.Nimba
import com.nimba.mapreducer.mapper.config.Transformation
import com.nimba.mapreducer.constants.MapperServiceTypes._
import com.nimba.mapreducer.constants.ReducerServiceTypes._
import com.nimba.mapreducer.reducer.config.Operation

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

        val reducerBuilder = Nimba.startNewReducer()
        reducerBuilder
            .outputLocation("output/task_1/data")
            .outputFormat("csv")
            .data(mapperBuilder)
            .outputDelimiter(",")
            .transformations(
                Operation(
                    transformation_type = COUNT_ROWS,
                    params = Map(
                        GROUP_COL_ALIAS -> "date"
                    )
                )
            )
        Nimba.finilizeReducer(reducerBuilder)
        // Nimba.start()
            
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
        
        val reducerBuilder = Nimba.startNewReducer()
        reducerBuilder
            .outputLocation("output/task_2/data")
            .outputFormat("csv")
            .outputDelimiter(",")
            .data(mapperBuilder)
            .join(secondMapper, INNER_JOIN)

        Nimba.finilizeMapper(mapperBuilder)
        Nimba.finilizeMapper(secondMapper)
        Nimba.finilizeReducer(reducerBuilder)
            
    }
     
    // Main method
    def main(args: Array[String]): Unit =
    {
        task_1()
        task_2()
        Nimba.start()
        
    }
}