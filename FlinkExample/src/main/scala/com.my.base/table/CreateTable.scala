package com.my.base.table

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sources.CsvTableSource

/**
  * @Author: Yuan Liu
  * @Description:
  * @Date: Created in 16:56 2020/3/22
  *
  *        Good Good Study Day Day Up
  */
object CreateTable {

  def main(args: Array[String]): Unit = {
    //初始化Flink的Streaming（流计算）上下文执行环境
    val streamEnv: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
    //初始化Table API的上下文环境
    val tableEvn =StreamTableEnvironment.create(streamEnv)
    //创建CSV格式的TableSource
    val fileSource = new CsvTableSource("/station.log",
      Array[String]("f1","f2","f3","f4","f5","f6"),
      Array(Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.LONG,Types.LONG))
    //注册Table，表名为t_log
    tableEvn.registerTableSource("t_log",fileSource)
    //转换成Table对象，并打印表结构
    tableEvn.scan("t_log").printSchema()
  }

}
