package com.ghou.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
/**
 * @description: TODO
 * @author: OGH
 * @date: 2021-03-24 21:23
 */
object Demo01 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从外部命令获取参数
    val paramTool = ParameterTool.fromArgs(args)
    val host = paramTool.get("host")
    val port = paramTool.getInt("port")


    env.setParallelism(2)

//    val in = env.socketTextStream("localhost", 7777)

    val in = env.socketTextStream(host, port)

    val result = in.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    result.print("flink demo01")

    env.execute()
  }

}
