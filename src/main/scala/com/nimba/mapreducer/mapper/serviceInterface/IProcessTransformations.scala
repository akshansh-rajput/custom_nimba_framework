package com.nimba.mapreducer.mapper.serviceInterface

import com.nimba.mapreducer.mapper.config.Transformation

abstract class IProcessTransformations
{
    def applyTransformations(transformationConfig: Map[String, Transformation] , data: Seq[Map[String, String]]): Seq[Map[String, String]]

    def applyGrouping(config: Transformation, data: Seq[Map[String, String]]): Map[String,Seq[Map[String, String]]]
}