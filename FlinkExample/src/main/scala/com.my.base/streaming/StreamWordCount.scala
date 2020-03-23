package com.my.base.streaming

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Author: Yuan Liu
  * @Description: 流式处理
  * @Date: Created in 11:20 2019/7/16
  *
  *        nc -lk 8888
  *        Good Good Study Day Day Up
  */
object StreamWordCount {

  def main(args: Array[String]): Unit = {

    // 从外部命令中获取参数
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    // 初始化Flink的Streaming（流计算）上下文执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //导入隐式转换，建议写在这里，可以防止IDEA代码提示出错的问题
    import org.apache.flink.streaming.api.scala._

    // 接收socket文本流
    val textDstream: DataStream[String] = env.socketTextStream(host, port).setParallelism(2)

    // flatMap和Map需要引用的隐式转换 // keyBy 与groupBy
    val dataStream: DataStream[(String, Int)] = textDstream.flatMap(_.split("\\s")).filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)

    dataStream.print().setParallelism(1)

    // 启动流式处理，如果没有该行代码上面的程序不会运行
    env.execute("Socket stream word count")
  }
}
