package com.nimba.status.interface

abstract class IStatusService
{
    def updateStatus(id: String, path: String, status: String): Unit

    def readStatus(id: String, path: String): Map[String, String]
}