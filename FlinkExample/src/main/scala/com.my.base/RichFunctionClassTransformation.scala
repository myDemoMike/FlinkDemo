package com.my.base

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.my.base.source.StationLog
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Author: Yuan Liu
  * @Description: 自定义复函数类
  * @Date: Created in 23:24 2020/3/20
  *
  *        Good Good Study Day Day Up
  */
object RichFunctionClassTransformation {
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

    //过滤那些通话成功的
    data.filter(_.callType.equals("success"))
      .map(new CallRichMapFunction())
      .print()
    streamEnv.execute()
  }

  //自定义的富函数类
  class CallRichMapFunction() extends RichMapFunction[StationLog, StationLog] {
    var conn: Connection = _
    var pst: PreparedStatement = _

    //生命周期管理，初始化的时候创建数据连接
    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://localhost/test", "root", "123123")
      pst = conn.prepareStatement("select name from t_phone where phone_number =?")
    }

    override def map(in: StationLog): StationLog = {
      //查询主叫用户的名字
      pst.setString(1, in.callOut)
      val set1: ResultSet = pst.executeQuery()
      if (set1.next()) {
        in.callOut = set1.getString(1)
      }
      //查询被叫用户的名字
      pst.setString(1, in.callInt)
      val set2: ResultSet = pst.executeQuery()
      if (set2.next()) {
        in.callInt = set2.getString(1)
      }
      in
    }
    //关闭连接
    override def close(): Unit = {
      pst.close()
      conn.close()
    }
  }
}
