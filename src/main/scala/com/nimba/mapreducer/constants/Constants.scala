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
    
}

object InternalConstants
{
    val JOB_INVOKE_TEMPLATE: String = "java -cp target/nimba-framework-1.0.0-SNAPSHOT-jar-with-dependencies.jar"
    val INTERNAL_LOCATION: String = "mapreducer_internal"
    val MAPPER_CONFIG_LOCATION: String = f"${INTERNAL_LOCATION}/mapper"
    val MAPPER_JOB_INVOKER: String = f"${JOB_INVOKE_TEMPLATE} com.nimba.mapreducer.mapper.MapperRunner ${MAPPER_CONFIG_LOCATION}"
    val STATUS_LOCATION: String = f"${INTERNAL_LOCATION}/status"
    val MAPPER_STATUS_LOCATION: String = f"${STATUS_LOCATION}/mapper"
}