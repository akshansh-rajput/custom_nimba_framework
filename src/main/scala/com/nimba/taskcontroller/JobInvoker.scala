package com.nimba.taskcontroller

import scala.sys.process._

object JobInvoker
{
    def invokeJob(jobInvokeCommand: String): Unit = {

        f"${jobInvokeCommand} &".!
    }
}