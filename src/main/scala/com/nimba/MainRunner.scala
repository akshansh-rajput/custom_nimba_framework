package com.nimba
object MainRunner
{
     
    // Main method
    def main(args: Array[String]): Unit =
    {
        import scala.sys.process._
        println("STARTING 1")
        "java -cp target/nimba-framework-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.nimba.mapreducer.mapper.MapperRunner mapreducer_int/mapper e1".!
        println("Starting")
        "java -cp target/nimba-framework-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.nimba.mapreducer.mapper.MapperRunner mapreducer_int/mapper e1".!
    }
}