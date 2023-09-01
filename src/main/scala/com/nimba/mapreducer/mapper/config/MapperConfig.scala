package com.nimba.mapreducer.mapper.config

case class Transformation (
    transformation_type: String,
    params: Map[String, Any]
)

case class MapperTransformations (
    data_source: String,
    data_format: String,
    data_delimiter: String,
    intrim_loc: String,
    output_loc: String,
    status_loc: String,
    table: String,
    include_header: String = "true",
    transformations: Map[String, Transformation],
    group: Transformation
)