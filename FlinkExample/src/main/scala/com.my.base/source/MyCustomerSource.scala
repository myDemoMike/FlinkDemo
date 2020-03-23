package com.my.base.source

import java.util.Random

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Author: Yuan Liu
  * @Description:     自定义source 区分SourceFunction和ParallelSourceFunction、RichParallelSourceFunction  并行度有关的问题  readme.md
  * @Date: Created in 13:21 2020/3/18
  *
  *        Good Good Study Day Day Up
  */

/**
  * 基站日志
  *
  * @param sid      基站的id
  * @param callOut  主叫号码
  * @param callInt  被叫号码
  * @param callType 呼叫类型
  * @param callTime 呼叫时间 (毫秒)
  * @param duration 通话时长 （秒）
  */
case class StationLog(sid: String, var callOut: String, var callInt: String, callType: String, callTime: Long, duration: Long)


class MyCustomerSource extends SourceFunction[StationLog] {
  //是否终止数据流的标记
  var flag = true;

  /**
    * 主要的方法
    * 启动一个Source
    * 大部分情况下，都需要在这个run方法中实现一个循环，这样就可以循环产生数据了
    *
    * @param sourceContext
    * @throws Exception
    */
  override def run(sourceContext: SourceFunction.SourceContext[StationLog]):
  Unit = {
    val random = new Random()
    var types = Array("fail", "busy", "barring", "success")
    while (flag) { //如果流没有终止，继续获取数据
      1.to(5).map(i => {
        var callOut = "1860000%04d".format(random.nextInt(10000))
        var callIn = "1890000%04d".format(random.nextInt(10000))
        new
            StationLog("station_" + random.nextInt(10), callOut, callIn, types(random.nextInt(4
            )), System.currentTimeMillis(), 0)
      }).foreach(sourceContext.collect(_)) //发数据
      Thread.sleep(2000) //每发送一次数据休眠2秒
    }
  }
  //终止数据流
  override def cancel(): Unit = flag = false

}


object CustomerSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    val stream: DataStream[StationLog] = env.addSource(new
        MyCustomerSource)
    stream.print()
    env.execute()
  }
}
