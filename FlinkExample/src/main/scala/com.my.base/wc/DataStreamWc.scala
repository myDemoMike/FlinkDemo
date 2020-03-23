package com.my.base.wc

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  *  nc -lk  1111
  *
  * @Author: Yuan Liu
  * @Description:
  * @Date: Created in 21:35 2019/9/8
  *
  *        Good Good Study Day Day Up
  *
  */
object DataStreamWc {


  def main(args: Array[String]): Unit = {

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[String] = env.socketTextStream("127.0.0.1",1111)

    val sumDstream :DataStream[(String,Int)] = dataStream.flatMap(_.split(" ")).filter(_.nonEmpty)
      .map((_,1)).keyBy(0).sum(1)

    sumDstream.print()
    env.execute()
  }
}
