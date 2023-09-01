package com.nimba.mapreducer.reducer.config

case class Operation(
    transformation_type: String
)

case class ReducerOperations(
    data_source: String,
    data_format: String,
    data_delimiter: String,
    output_loc: String,
    status_loc: String,
    transformations: Map[String, Operation]
)