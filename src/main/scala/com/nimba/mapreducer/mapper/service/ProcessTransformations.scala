package com.nimba.mapreducer.mapper.service

import com.nimba.mapreducer.mapper.serviceInterface.IProcessTransformations
import com.nimba.mapreducer.mapper.config.Transformation

class ProcessTransformations extends IProcessTransformations
{

    /**
     * Determines if the leftValue is equal to the rightValue.
     *
     * This method compares two values and returns `true` if the leftValue is equal to
     * the rightValue, otherwise it returns `false`.
     *
     * @param leftValue  The value to be compared as the left operand.
     * @param rightValue The value to be compared as the right operand.
     * @return `true` if leftValue is equal to rightValue, `false` otherwise.
     */
    def eq(leftValue: String, rightValue: String): Boolean = {
        leftValue.equalsIgnoreCase(rightValue)
    }


    /**
     * Determines if the left integer is less than the right integer.
     *
     * This method compares two integer values and returns `true` if the leftValue is less
     * than the rightValue, otherwise it returns `false`.
     *
     * @param leftValue  The integer value to be compared as the left operand.
     * @param rightValue The integer value to be compared as the right operand.
     * @return `true` if leftValue is less than rightValue, `false` otherwise.
     */
    def lt(leftValue: Int, rightValue: Int): Boolean = {
        leftValue < rightValue
    }

    /**
     * Determines if the left integer is greater than the right integer.
     *
     * This method compares two integer values and returns `true` if the leftValue is greater
     * than the rightValue, otherwise it returns `false`.
     *
     * @param leftValue  The integer value to be compared as the left operand.
     * @param rightValue The integer value to be compared as the right operand.
     * @return `true` if leftValue is greater than rightValue, `false` otherwise.
     */
    def gt(leftValue: Int, rightValue: Int): Boolean = {
        leftValue > rightValue
    }


    /**
     * Filters data based on a specified key, value, and condition.
     *
     * This method takes a data and apply the filter condition on each row. condition specified as follows:
     *
     * - "eq" (equals): Rows where the value of the specified key is equal to the given value.
     * - "lt" (less than): Rows where the value of the specified key is less than the given value.
     * - "gt" (greater than): Rows where the value of the specified key is greater than the given value.
     * - "le" (less than or equal to): Rows where the value of the specified key is less than or equal
     *   to the given value.
     * - "ge" (greater than or equal to): Rows where the value of the specified key is greater than or
     *   equal to the given value.
     *
     * @param data       The sequence of maps representing the data to filter.
     * @param key        The key in each map used for comparison.
     * @param value      The value to compare with.
     * @param condition  The condition to apply for filtering ("eq", "lt", "gt", "le", "ge").
     * @return A new sequence of maps containing only the rows that meet the specified condition.
     */
    def filterData(data: Seq[Map[String, String]], key: String, value: String, condition: String): Seq[Map[String, String]] = {
        val filterData = data.filter(row =>{
            val leftValue = row.get(key).get
            val result = condition match {
                case "eq" => 
                    eq(leftValue, value)
                case "lt" =>
                    lt(leftValue.toInt, value.toInt)
                case "gt" =>
                    gt(leftValue.toInt, value.toInt)
                case "le" =>
                    lt(leftValue.toInt, value.toInt) || eq(leftValue, value)
                case "ge" =>
                    gt(leftValue.toInt, value.toInt) || eq(leftValue, value)
            }
            result            
        })
        filterData
    }

    /**
     * Group a sequence of maps by a specified key.
     *
     * This method takes a sequence of maps and groups them based on a specified key. The resulting Map
     * contains key-value pairs where the keys are distinct values found in the specified key of the maps,
     * and the values are sequences of maps that share the same key value.
     *
     * @param data The sequence of maps to be grouped.
     * @param key The key by which to group the maps.
     * @return A Map where keys represent distinct values of the specified key, and values are sequences of maps.
     */
    def grouping(data: Seq[Map[String, String]], key: String): Map[String, Seq[Map[String, String]]] = {
        data.groupBy(_.get(key).get)
    }

    /**
     * Apply a series of transformations to a sequence of maps.
     *
     * This method applies a series of transformations specified in the transformationConfig map to a sequence of maps data.
     * Each transformation is identified by a unique key with the prefix "t_" in the transformationConfig map. Supported transformation
     * types include "filter" and others. The order of transformations is determined by the numeric suffixes in the keys.
     *
     * @param transformationConfig A map containing transformation configurations with keys prefixed by "t_".
     * @param data The sequence of maps to which transformations should be applied.
     * @return A sequence of maps after applying the specified transformations.
     */
    def applyTransformations(transformationConfig: Map[String, Transformation], data: Seq[Map[String, String]]): Seq[Map[String, String]] = {
        val keyPrefix = "t_"
        if(transformationConfig == null || transformationConfig.isEmpty)
            return data
        val transformationCount = transformationConfig.size
        var transformData = data
        for(i <- 1 to transformationCount){
            val transformation = transformationConfig.get(keyPrefix + i).get
            val transType = transformation.transformation_type
            if(!transType.equalsIgnoreCase("group")){
                transformData = transType match {
                    case "filter" =>
                        val params = transformation.params
                        val key = params.get("col").get.asInstanceOf[String]
                        val value = params.get("value").get.asInstanceOf[String]
                        val condition = params.get("condition").get.asInstanceOf[String]
                        filterData(transformData, key, value, condition)
                }
            }
            // else{
            //     val params = transformation.params
            //     val key = params.get("key").get.asInstanceOf[String]
            //     return grouping(transformData, key)
            // }
        }
        transformData
    }

    /**
     * Apply grouping to a sequence of maps based on a specified key.
     *
     * This method applies a grouping transformation to the given sequence of maps data.
     *
     * @param config The configuration for the grouping transformation.
     * @param data The sequence of maps to be grouped.
     * @return A Map where keys represent distinct values of the specified key, and values are sequences of maps.
     */
    def applyGrouping(config: Transformation, data: Seq[Map[String, String]]): Map[String,Seq[Map[String, String]]] = {
        val params = config.params
        val key = params.get("key").get.asInstanceOf[String]
        grouping(data, key)
    }
}