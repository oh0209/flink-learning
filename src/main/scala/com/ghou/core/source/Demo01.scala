package com.ghou.core.source

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties


//定义样例类 温度传感器

case class SensorReading(id:String,timestamp:Long,temperature:Double)

/**
 * @description: TODO
 * @author: OGH
 * @date: 2021-04-12 21:07
 */
object Demo01 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataList = List(
      SensorReading("s11", 1541199889, 36.1),
      SensorReading("s8", 1541199832, 36.2),
      SensorReading("s22", 1541199844, 39.1),
      SensorReading("s17", 1541199898, 19.1),
      SensorReading("s33", 1541199811, 49.1),
      SensorReading("s20", 1541199834, 28.1)
    )
    //读集合
    val result = env.fromCollection(dataList)
    result.print()
    //读文件
    val filePath = "E:\\guohai\\work_idea\\my_github\\flink-learning\\data\\b.txt"
    val result2 = env.readTextFile(filePath)
    result2.print()

    //读kafka
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    val stream = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))

    stream.print()
    env.execute("test01")

  }

}
