package com.my.base.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Author: Yuan Liu
  * @Description:
  * @Date: Created in 14:57 2019/7/16
  *
  *        Good Good Study Day Day Up
  */
object FlinkWithCollection {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream1 = env
      .fromCollection(List(
        SensorReading("sensor_1", 1547718199, 35.80018327300259),
        SensorReading("sensor_6", 1547718201, 15.402984393403084),
        SensorReading("sensor_7", 1547718202, 6.720945201171228),
        SensorReading("sensor_10", 1547718205, 38.101067604893444)
      ))
    stream1.print("stream1:").setParallelism(1)
    env.execute()
  }

}

case class SensorReading(id: String, timestamp: Long, temperature: Double)