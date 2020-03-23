package com.my.base.table

import com.my.base.source.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
  * @Author: Yuan Liu
  * @Description:
  * @Date: Created in 17:02 2020/3/22
  *
  *        Good Good Study Day Day Up
  */
object TableAPITest {
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    //初始化Table API的上下文环境
    val tableEvn = StreamTableEnvironment.create(streamEnv)
    //导入隐式转换，建议写在这里，可以防止IDEA代码提示出错的问题
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._
    val data = streamEnv.readTextFile(getClass.getResource("/station.log").getPath)
      // val data = streamEnv.socketTextStream("hadoop101", 8888)
      .map(line => {
      var arr = line.split(",")
      new
          StationLog(arr(0).trim, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim.toLong, arr(5).trim.toLong)
    })

    val table: Table = tableEvn.fromDataStream(data)
    //查询
    tableEvn.toAppendStream[Row](
      table.select('sid, 'callType as 'type, 'callTime, 'callOut))
      .print()
    //过滤查询
    tableEvn.toAppendStream[Row](
      table.filter('callType === "success") //filter
        .where('callType === "success")) //where
      .print()

    val table2: Table = tableEvn.fromDataStream(data)
    // 在使用toRetractStream 方法时， 返回的数据类型结果为
    // DataStream[(Boolean,T)]，Boolean 类型代表数据更新类型，True对应INSERT操作更新的
    // 数据，False 对应DELETE 操作更新的数据。
    tableEvn.toRetractStream[Row](
      table2.groupBy('sid).select('sid, 'sid.count as 'logCount))
      .filter(_._1 == true) //返回的如果是true才是Insert的数据
      .print()
    tableEvn.execute("sql")
  }
}
