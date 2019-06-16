package com.my.base.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

/**
  * 包: com.kamluen.flink.scala
  * 开发者: wing
  * 开发时间: 2019-02-23
  * 功能：
  */
object TableFirstFunctionTest {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val student = env.fromElements(
      ("张三",1,78),
      ("李四",1,56),
      ("王五",1,86),
      ("麻六",2,93),
      ("郑七",2,92),
      ("周八",2,82)
    )
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerDataSet("student",student,'name,'grade,'score)
//    tEnv.registerFunction("first_value",new CountAggFunction)
//    tEnv.sqlQuery(
//      """
//        |select first_value(grade) from student
//      """.stripMargin).toDataSet[Double].print()
    val fullStudentScore = tEnv.sqlQuery(
      """
        |select
        |   s1.name name1,s1.grade grade,s1.score score1,s2.name name2,s2.score score2
        |from student s1
        |   left join student s2 on s1.grade = s2.grade
        |where s2.score < s1.score
      """.stripMargin)
    val fullStudentScoreDataSet = fullStudentScore.toDataSet[(String, Int, Int,String, Int)]
    /*fullStudentScoreDataSet.groupBy(0,1).sortGroup(4,Order.DESCENDING)
      .reduceGroup((it: Iterator[(String, Int, Int, String, Int)], collector:Collector[(String, Int, Int, String, Int)]) =>
        collector.collect(it.next())
      ).print()*/
    val fullStudentScore2 = tEnv.sqlQuery(
    """
        |select
        |t.name,t.grade,t.score,
        | (select
        |   s1.score score1
        |   from student s1
        |   where s1.grade = t.grade and s1.score < t.score
        |     order by s1.score desc
        |   limit 1 offset 0
        |   ) score1
        |from student t
        |where t.score > (select
        |   s1.score score1
        |   from student s1
        |   where s1.grade = t.grade and s1.score < t.score
        |     order by s1.score desc
        |   limit 1 offset 0
        |   )
      """.stripMargin)
    fullStudentScore2.toDataSet[(String, Int, Int, Int)].print()
    tEnv.registerTable("full_student_score",fullStudentScore)
    tEnv.sqlQuery(
      """
        |select
        |   s1.name name1,s1.grade grade,s1.score score1,s2.name name2,s2.score score2
        |from student s1
        |   left join student s2 on s1.grade = s2.grade
        |where
        |    s2.score < s1.score
        |    and s2.score >=
        |     //比它少的数值
        |    (select score from student s
        |         where s.grade = s1.grade
        |         and s.score < s1.score
        |         order by s.score desc limit 1 offset 1)
        |
      """.stripMargin)
//      .toDataSet[(String, Int, Int,String, Int)].print()
  }

}
