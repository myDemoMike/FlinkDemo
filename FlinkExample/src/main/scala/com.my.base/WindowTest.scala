package com.my.base

import com.my.base.source.StationLog
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @Author: Yuan Liu
  * @Description:
  * @Date: Created in 14:55 2020/3/22
  *
  *        Good Good Study Day Day Up
  */
object WindowTest {

  def main(args: Array[String]): Unit = {
    //初始化Flink的Streaming（流计算）上下文执行环境
    val streamEnv: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    //导入隐式转换，建议写在这里，可以防止IDEA代码提示出错的问题
    import org.apache.flink.streaming.api.scala._
    //读取文件数据
    val data = streamEnv.readTextFile(getClass.getResource("/station.log").getPath)
      .map(line => {
        val arr = line.split(",")
        new
            StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })
    //每隔5秒统计每个基站的日志数量
    data.map(stationLog => ((stationLog.sid, 1)))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .reduce((v1, v2) => (v1._1, v1._2 + v2._2))

    //每隔3秒计算最近5秒内，每个基站的日志数量
    data.map(stationLog => ((stationLog.sid, 1)))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5), Time.seconds(3))
      .aggregate(new AggregateFunction[(String, Int), (String, Long), (String, Long)] {

        override def createAccumulator() = ("", 0)

        override def add(in: (String, Int), acc: (String, Long)) = {
          (in._1, acc._2 + in._2)
        }
        override def getResult(acc: (String, Long)) = acc

        override def merge(acc: (String, Long), acc1: (String, Long)) = {
          (acc._1, acc1._2 + acc._2)
        }
      })

    //每隔5秒统计每个基站的日志数量
    data.map(stationLog => ((stationLog.sid, 1)))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .process(new
          ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String,
          Int)], out: Collector[(String, Int)]): Unit = {
          println("-------")
          out.collect((key, elements.size))
        }
      })
      .print()
  }
}
