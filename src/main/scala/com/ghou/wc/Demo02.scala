package com.ghou.wc

import org.apache.flink.api.scala._

/**
 * @description: TODO
 * @author: OGH
 * @date: 2021-03-24 22:18
 */
object Demo02 {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val source = env.readTextFile("data/a.txt")
    val result = source
      .flatMap(_.split(","))
      .map((_,1))
      .groupBy(0)
      .sum(1)

    result.print()
  }

}
