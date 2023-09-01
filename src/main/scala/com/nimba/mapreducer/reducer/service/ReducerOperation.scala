package com.nimba.mapreducer.reducer.service

import com.nimba.mapreducer.reducer.serviceInterface.IReducerOperation
import com.nimba.mapreducer.reducer.config.Operation
import com.nimba.mapreducer.reducer.serviceInterface.IReducerService
import com.nimba.mapreducer.reducer.service.ReducerService
import scala.collection.mutable.ListBuffer


class ReducerOperation extends IReducerOperation
{

    def countRows(data: Seq[Map[String, Seq[Map[String, String]]]], key: String): Seq[Map[String, String]] = {
        val result =scala.collection.mutable.Map[String, Int]()
        data.foreach(row =>{
            val id = row.keySet.toSeq(0)
            result(id) = result.getOrElse(id, 0) + row.get(id).get.size
        })
        val enRichResult = result.toMap[String, Int].map(x=> Map(key -> x._1 , "Count" -> x._2.toString)).toSeq
        enRichResult.asInstanceOf[Seq[Map[String,String]]]
    }

    def readLeftSource(sourcePaths: Seq[String]): Seq[Map[String,Seq[Map[String, String]]]] = {
        val reader: IReducerService = new ReducerService()
        reader.readData(sourcePaths,Map())

    }

    def inner(rightData: Seq[Map[String, Seq[Map[String, String]]]], leftData: Seq[Map[String, Seq[Map[String, String]]]]): Seq[Map[String, String]] = {
        val rightIds = rightData.map(x=> x.keySet.toSeq(0))
        val leftIds = leftData.map(x=> x.keySet.toSeq(0))
        val commonIds = rightIds.intersect(leftIds)
        val filterRightData = rightData.filter(x=> commonIds.contains(x.keySet.toSeq(0)))
        val filterLeftData = leftData.filter(x=> commonIds.contains(x.keySet.toSeq(0)))
        // Merging two data
        val result = ListBuffer[Map[String,String]]()
        filterRightData.foreach(row =>{
            val currentId = row.keySet.toSeq(0)
            val values = row.get(currentId).get
            values.foreach(innerrow=>{
                val reqLeftData = filterLeftData.filter(x=> x.keySet.toSeq(0).equalsIgnoreCase(currentId))
                reqLeftData.foreach(leftRow=>{
                    val leftValue = leftRow.get(currentId).get
                    leftValue.foreach(innerLeft=>{
                        val reqData = innerrow ++ innerLeft
                        result += reqData
                    })
                })
            })
        })
        result.toSeq
    }

    def join(data: Seq[Map[String, Seq[Map[String, String]]]], leftSource: String, joinType: String): Seq[Map[String, String]] = {
        val sourcePaths = leftSource.split(",").toSeq
        val leftData = readLeftSource(sourcePaths)
        inner(data, leftData)
    }


    def applyOperations(operations: Operation, data: Seq[Map[String, Seq[Map[String, String]]]]): Seq[Map[String, String]] = {
        val opersPrefix = "t_"
        val transformationType = operations.transformation_type
        if(transformationType.equalsIgnoreCase("count_row")){
            return countRows(data, operations.params.get("key").get)
        }
        else{
            join(data, operations.params.get("left_source").get, operations.params.get("type").get)
        }
        
    }
}