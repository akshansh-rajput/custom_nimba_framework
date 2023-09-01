package com.nimba.mapreducer.reducer.serviceInterface

import com.nimba.mapreducer.reducer.config.Operation


abstract class IReducerOperation
{
    def applyOperations(operations: Operation, data: Seq[Map[String, Seq[Map[String, String]]]]): Seq[Map[String, String]]
}
