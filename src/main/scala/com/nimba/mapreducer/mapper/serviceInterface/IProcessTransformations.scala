package com.nimba.mapreducer.mapper.serviceInterface

import com.nimba.mapreducer.mapper.config.Transformation

abstract class IProcessTransformations
{
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
    def applyTransformations(transformationConfig: Map[String, Transformation] , data: Seq[Map[String, String]]): Seq[Map[String, String]]

    /**
     * Apply grouping to a sequence of maps based on a specified key.
     *
     * This method applies a grouping transformation to the given sequence of maps data.
     *
     * @param config The configuration for the grouping transformation.
     * @param data The sequence of maps to be grouped.
     * @return A Map where keys represent distinct values of the specified key, and values are sequences of maps.
     */
    def applyGrouping(config: Transformation, data: Seq[Map[String, String]]): Map[String,Seq[Map[String, String]]]
}