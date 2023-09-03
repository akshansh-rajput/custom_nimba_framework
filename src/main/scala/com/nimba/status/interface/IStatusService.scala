package com.nimba.status.interface

abstract class IStatusService
{
    /**
     * Save status information of mapper and reducer to a JSON file at the specified location.
     *
     * This method creates a JSON file of the status information at the specified
     * sinkLocation.
     *
     * @param id The identifier used as the filename (without the ".json" extension).
     * @param path The directory where the JSON file should be saved.
     * @param status The status information to be saved.
     */
    def updateStatus(id: String, path: String, status: String): Unit

    /**
     * Read status information of mapper/reducer from a JSON file with the specified identifier.
     *
     * This method reads status information from a JSON file located at the specified path, where the filename is based
     * on the provided id. If the file exists, it is deserialized into a Map containing status information.
     * If the file does not exist, a default status of "pending" is returned.
     *
     * @param id The identifier used to construct the filename (without the ".json" extension).
     * @param path The directory where the JSON file is expected to be located.
     * @return A Map containing the status information or a default "pending" status if the file does not exist.
     */
    def readStatus(id: String, path: String): Map[String, String]
}