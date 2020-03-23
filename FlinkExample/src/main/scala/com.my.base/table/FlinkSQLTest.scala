package com.my.base.table

import com.my.base.source.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.types.Row

/**
  * @Author: Yuan Liu
  * @Description:
  * @Date: Created in 17:19 2020/3/22
  *
  *        Good Good Study Day Day Up
  */
object FlinkSQLTest {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment

    //导入隐式转换，建议写在这里，可以防止IDEA代码提示出错的问题
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._

    //读取数据
    val data = streamEnv.readTextFile(getClass.getResource("/station.log").getPath)
      .map(line => {
        var arr = line.split(",")
        new
            StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
      })

    val tableEvn = StreamTableEnvironment.create(streamEnv)
    // 第一种sql调用方式
    val table: Table = tableEvn.fromDataStream(data)
    //执行sql
    val result1: Table = tableEvn.sqlQuery(s"select sid,sum(duration) as sd from $table where callType = 'success ' group by sid")
    //打印结果
    tableEvn.toRetractStream[Row](result1)
      .filter(_._1 == true)
      .print()

    //第二种sql调用方式
    tableEvn.registerDataStream("t_station_log", data)
    val result2: Table = tableEvn.sqlQuery("select sid ,sum(duration) as sd from t_station_log where callType = 'success ' group by sid")
    tableEvn.toRetractStream[Row](result2)
      .filter(_._1 == true)
      .print()
    tableEvn.execute("sql_api")


  }
}
