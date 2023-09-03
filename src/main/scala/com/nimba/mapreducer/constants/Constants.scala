package com.nimba.mapreducer.constants

object MapperServiceTypes
{
    val FILTER_DATA: String = "filter"
    val GROUP_DATA: String = "group"
    val GROUP_ON_KEY: String = "key"
    val COLUMN: String = "col"
    val CHECK: String = "condition"
    val CHECK_VALUE: String = "value"
    val EQUAL_CHECK: String = "eq"
    val LESS_THAN_CHECK: String = "lt"
    val GREATER_THAN_CHECK: String = "gt"
    val LESS_THAN_EQUAL_CHECK: String = "le"
    val GREATER_THAN_EQUAL_CHECK: String = "ge"
}

object ReducerServiceTypes
{
    val INNER_JOIN: String = "inner"
    val LEFT_SOURCE_PATH_KEY: String = "left_source"
    val JOIN_OPERATION: String = "join"
    val JOIN_TYPE_KEY: String = "type"
    val JOIN_ON_KEY: String = "key"
    val COUNT_ROWS: String = "count_row"
    val GROUP_COL_ALIAS: String = "key"
    val LEFT_SOURCE_KEY: String = "left_source"
    
}

object InternalConstants
{
    val JOB_INVOKE_TEMPLATE: String = "java -cp target/nimba-framework-1.0.0-SNAPSHOT-jar-with-dependencies.jar"
    val INTERNAL_LOCATION: String = "mapreducer_internal"
    val INTRIM_DATA_LOCATION: String = "mapreducer_internal/intrim_data"
    val MAPPER_CONFIG_LOCATION: String = f"${INTERNAL_LOCATION}/mapper"
    val REDUCER_CONFIG_LOCATION: String = f"${INTERNAL_LOCATION}/reducer"
    val SPLITTED_DATA_LOCATION: String = f"${INTERNAL_LOCATION}/splitteddata"
    val MAPPER_JOB_INVOKER: String = f"${JOB_INVOKE_TEMPLATE} com.nimba.mapreducer.mapper.MapperRunner ${MAPPER_CONFIG_LOCATION}"
    val REDUCER_JOB_INVOKER: String = f"${JOB_INVOKE_TEMPLATE} com.nimba.mapreducer.reducer.ReducerRunner ${REDUCER_CONFIG_LOCATION}"
    val STATUS_LOCATION: String = f"${INTERNAL_LOCATION}/status"
    val MAPPER_STATUS_LOCATION: String = f"${STATUS_LOCATION}/mapper"
    val REDUCER_STATUS_LOCATION: String = f"${STATUS_LOCATION}/reducer"
    val STORAGE_LAYER: String = "storage_layer"
    val WORKER_ID: String = "worker_id"
    val DELIMITER: String = "delimiter"
    val OUTPUT_STORAGE_LAYER: String = "output"
    val MAX_ROW_PER_FILE: Int = 10
}