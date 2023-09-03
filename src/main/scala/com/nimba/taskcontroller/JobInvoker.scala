package com.nimba.taskcontroller

import scala.sys.process._

object JobInvoker
{
    /**
     * It start new mapper or reducer job in backgroup.
     * 
     * It execute command line and start multiple jobs in parallel
     * 
     * @param jobInvokeCommand cli command to execute
     */ 
    def invokeJob(jobInvokeCommand: String): Unit = {
        val logger = ProcessLogger(_ => ())
        f"${jobInvokeCommand} &"run(logger)
    }
}