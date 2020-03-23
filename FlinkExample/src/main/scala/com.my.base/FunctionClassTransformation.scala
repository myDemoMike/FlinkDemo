package com.my.base

import java.text.SimpleDateFormat
import java.util.Date

import com.my.base.source.StationLog
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Author: Yuan Liu
  * @Description:
  * @Date: Created in 23:13 2020/3/20StationLog
  *
  *        Good Good Study Day Day Up
  */
object FunctionClassTransformation {
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
        var arr = line.split(",")
        new
            StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })
    //定义时间输出格式
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //过滤那些通话成功的
    data.filter(_.callType.equals("success"))
      .map(new CallMapFunction(format))
      .print()
    streamEnv.execute()
  }

  //自定义的函数类
  class CallMapFunction(format: SimpleDateFormat) extends

    MapFunction[StationLog, String] {
    override def map(t: StationLog): String = {
      var strartTime = t.callTime;
      var endTime = t.callTime + t.duration * 1000
      "主叫号码:" + t.callOut + ",被叫号码:" + t.callInt + ",呼叫起始时间:" + format.format(new Date(strartTime)) + ",呼叫结束时间:" + format.format(new Date(endTime))
    }
  }

}
