package com.my.base.demo

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._


object SubQuery {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val quoteData = env.fromElements(
      ("000001.SH","2019-05-01",1),
      ("000001.SH","2019-05-06",2),
      ("000001.SH","2019-05-11",3),
      ("000001.SH","2019-05-16",4),
      ("000001.SH","2019-05-21",5),
      ("000002.SH","2018-12-01",51),
      ("000002.SH","2018-12-06",52),
      ("000002.SH","2018-12-11",53),
      ("000002.SH","2018-12-16",54),
      ("000002.SH","2018-12-21",55)
    )
    val adjustFactorData = env.fromElements(
      ("000001.SH","2019-05-03",0.33),
      ("000001.SH","2019-05-13",1.33),
      ("000002.SH","2018-12-03",0.44),
      ("000002.SH","2018-12-13",1.44)
    )
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerDataSet("quote",quoteData,'asset_id,'date,'open)
    tEnv.registerDataSet("adjust_factor",adjustFactorData,'asset_id,'ex_divi_date,'factor)

    tEnv.sqlQuery(
      """
        |select q.asset_id,q.`date`,a.ex_divi_date,a.factor
//        |   ,(select ex_divi_date from
//        |       adjust_factor a2
//        |    where a2.asset_id = q.asset_id
//        |
//        |       order by ex_divi_date desc limit 1)
        |from quote q
        |left join adjust_factor a on a.asset_id = q.asset_id
//        |where a.ex_divi_date =
//        |      (select ex_divi_date from adjust_factor a2 where a2.asset_id = q.asset_id and a2.ex_divi_date < q.`date` order by ex_divi_date desc limit 1)
      """.stripMargin).toDataSet[(String, String, String, Double)].print()
  }
}
