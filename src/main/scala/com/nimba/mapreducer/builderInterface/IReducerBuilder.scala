package com.nimba.mapreducer.builderInterface



abstract class IReducerBuilder
{

    def dataSource(path: String): this.type

    def dataFormat(format: String): this.type

    def dataDelimiter(delimiter: String): this.type

    // def transformations(transformation: Transformation): this.type

    def show(): Unit

    // def getReducerConfig(): MapperTransformations
   
}