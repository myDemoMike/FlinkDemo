package com.my.base

import com.my.base.source.StationLog
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector


/**
  * @Author: Yuan Liu
  * @Description:     把呼叫成功的Stream（主流）和不成功的Stream（侧流）分别输出。
  * @Date: Created in 23:32 2020/3/20
  *
  *        Good Good Study Day Day Up
  */
object TestSideOutputStream {
  //导入隐式转换，建议写在这里，可以防止IDEA代码提示出错的问题
  import org.apache.flink.streaming.api.scala._

  //侧输出流首先需要定义一个流的标签
  var notSuccessTag = new OutputTag[StationLog]("not_success")

  def main(args: Array[String]): Unit = {
    //初始化Flink的Streaming（流计算）上下文执行环境
    val streamEnv: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment

    streamEnv.setParallelism(1)
    //读取文件数据
    val data = streamEnv.readTextFile(getClass.getResource("/station.log").getPath)
      .map(line => {
        var arr = line.split(",")
        new
            StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })
    val mainStream: DataStream[StationLog] = data.process(new
        CreateSideOutputStream(notSuccessTag))
    //得到侧流
    val sideOutput: DataStream[StationLog] =
      mainStream.getSideOutput(notSuccessTag)
    mainStream.print("main")
    sideOutput.print("sideoutput")
    streamEnv.execute()
  }

  class CreateSideOutputStream(tag: OutputTag[StationLog]) extends
    ProcessFunction[StationLog, StationLog] {
    override def processElement(value: StationLog, ctx: ProcessFunction[StationLog,
      StationLog]#Context, out: Collector[StationLog]): Unit = {
      if (value.callType.equals("success")) {
        //输出主流
        out.collect(value)
      } else {
        //输出侧流
        ctx.output(tag, value)
      }
    }
  }


}
