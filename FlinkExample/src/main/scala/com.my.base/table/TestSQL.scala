package com.my.base.table

import com.my.base.source.StationLog
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
  * @Author: Yuan Liu
  * @Description: Flink SQL 也支持三种窗口类型，分别为Tumble Windows、HOP Windows 和Session
  *               Windows，其中HOP Windows 对应Table API 中的Sliding Window，同时每种窗口分别有相
  *               应的使用场景和方法。
  *
  *          统计最近每5 秒中内，每个基站的通话成功时间总和：
  * @Date: Created in 17:24 2020/3/22
  *
  *        Good Good Study Day Day Up
  */
object TestSQL {
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
    //指定EventTime为时间语义
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.setParallelism(1)
    //初始化Table API的上下文环境
    val tableEvn = StreamTableEnvironment.create(streamEnv)
    //导入隐式转换，建议写在这里，可以防止IDEA代码提示出错的问题
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._

    streamEnv.readTextFile(getClass.getResource("/station.log").getPath)
    val data = streamEnv.socketTextStream("10.33.11.11", 1111)
      .map(line => {
        var arr = line.split(",")
        new
            StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })
      .assignTimestampsAndWatermarks( //引入Watermark
        new
            BoundedOutOfOrdernessTimestampExtractor[StationLog](Time.seconds(2)) {
          //延迟2秒
          override def extractTimestamp(element: StationLog) = {
            element.callTime
          }
        })
//    //滚动窗口,窗口大小为5秒，需求：统计每5秒内，每个基站的成功通话时长总和
//    tableEvn.registerDataStream("t_station_log", data, 'sid, 'callOut, 'callIn, 'callType, 'callTime.rowtime, 'duration)
//    var result = tableEvn.sqlQuery(
//      "select sid ,sum(duration) from t_station_log where callType='success' group by tumble(callTime, interval'5' second), sid"
//    )
//    tableEvn.toRetractStream[Row](result)
//      .filter(_._1 == true)
//      .print()
//    tableEvn.execute("sql_api")


    //滑动窗口，窗口大小10秒，步长5秒，需求：每隔5秒，统计最近10秒内，每个基站通话成功时长总和
    tableEvn.registerDataStream ("t_station_log", data, 'sid, 'callType, 'callTime.rowtime, 'duration)
    var result = tableEvn.sqlQuery ("select sid ,sum(duration) , hop_start(callTime,interval '5' second,interval '10' second) as winStart, " +
    "hop_end(callTime,interval '5' second,interval '10' second) as winEnd " +
      "from t_station_log where callType='success' " +
      "group by hop(callTime,interval '5' second,interval '10' second),sid")
    tableEvn.toRetractStream[Row] (result) //打印每个窗口的起始时间
      .filter (_._1 == true)
      .print ()
    tableEvn.execute ("sql_api")
  }
}



