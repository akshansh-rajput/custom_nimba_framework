package com.nimba.mapreducer.reducer.service

import com.nimba.mapreducer.reducer.serviceInterface.IReducerOperation
import com.nimba.mapreducer.reducer.config.Operation
import scala.collection.mutable.ListBuffer


class ReducerOperation extends IReducerOperation
{

    /**
     * Count the number of rows in each subsequence of maps and return the results.
     *
     * This method takes a sequence of subsequences of maps data and counts the number of rows in each subsequence based on the specified key.
     * The result is a sequence of maps, where each map contains the original key and the corresponding row count as "Count."
     *
     * @param data The sequence of subsequences of maps to be counted.
     * @param key The key used for grouping and counting rows within each subsequence.
     * @return A sequence of maps containing the `key` and row count for each subsequence.
     */
    def countRows(data: Seq[Map[String, Seq[Map[String, String]]]], key: String): Seq[Map[String, String]] = {
        val result =scala.collection.mutable.Map[String, Int]()
        data.foreach(row =>{
            val id = row.keySet.toSeq(0)
            result(id) = result.getOrElse(id, 0) + row.get(id).get.size
        })
        val enRichResult = result.toMap[String, Int].map(x=> Map(key -> x._1 , "Count" -> x._2.toString)).toSeq
        enRichResult.asInstanceOf[Seq[Map[String,String]]]
    }

    /**
     * Perform an inner join operation between two sequences of maps based on common keys.
     *
     * This method performs an inner join operation between two sequences of maps, rightData and leftData,
     * based on common keys. For now, It cam only perform inner join.
     *
     * @param rightData The first sequence of maps to be joined.
     * @param leftData The second sequence of maps to be joined.
     * @return A sequence of merged maps resulting from the inner join operation.
     */
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

    /**
     * Perform an inner join operation between two sequences of maps based on common keys.
     *
     * This method performs an inner join operation between two sequences of maps, rightData and leftData,
     * based on common keys. For now, It cam only perform inner join.
     *
     * @param data The first sequence of maps to be joined.
     * @param leftData The second sequence of maps to be joined.
     * @param joinType nature of join
     * @return A sequence of merged maps resulting from the inner join operation.
     */
    def join(data: Seq[Map[String, Seq[Map[String, String]]]], leftData: Seq[Map[String, Seq[Map[String, String]]]], joinType: String): Seq[Map[String, String]] = {
        inner(data, leftData)
    }


    /**
     * Apply a specified operation on data.
     *
     * This method applies a specified operation, represented by the operations parameter, on data.
     * The type of operation to be applied is determined by the `transformation_type` field in the `operations` parameter.
     * The method supports various operations, including counting rows based on a key.
     *
     * @param operations The operation to apply, including its type and parameters.
     * @param data The sequence of maps to which the operation should be applied.
     * @return A sequence of maps resulting from applying the specified operation.
     */
    def applyOperations(operations: Operation, data: Seq[Map[String, Seq[Map[String, String]]]]): Seq[Map[String, String]] = {
        val opersPrefix = "t_"
        val transformationType = operations.transformation_type
        // if(transformationType.equalsIgnoreCase("count_row")){
            return countRows(data, operations.params.get("key").get)
        // }

        
    }
    
}