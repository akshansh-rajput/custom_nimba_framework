package com.nimba.mapreducer.reducer.serviceInterface

import com.nimba.mapreducer.reducer.config.Operation


abstract class IReducerOperation
{
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
    def applyOperations(operations: Operation, data: Seq[Map[String, Seq[Map[String, String]]]]): Seq[Map[String, String]]

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
    def join(data: Seq[Map[String, Seq[Map[String, String]]]], leftData: Seq[Map[String, Seq[Map[String, String]]]], joinType: String): Seq[Map[String, String]]
}
