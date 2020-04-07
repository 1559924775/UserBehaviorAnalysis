package com.uestc

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object TxMatchByJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderEventStream =
          env.readTextFile("D:\\IDEA\\UserBehaviorAnalysis\\OrderSystem\\src\\main\\resources\\OrderLog.csv")
//      env.socketTextStream("180.76.169.198",7778)
        .map( data => { val dataArray = data.split(",")
          OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
        })
        .filter(_.txId != "")
        .assignAscendingTimestamps(_.eventTime * 1000L)
        .keyBy(_.txId)


    val receiptEventStream =
          env.readTextFile("D:\\IDEA\\UserBehaviorAnalysis\\OrderSystem\\src\\main\\resources\\ReceiptLog.csv")
//      env.socketTextStream("180.76.169.198",7779)
        .map( data => {
          val dataArray = data.split(",")
          ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
        })
        .assignAscendingTimestamps(_.eventTime * 1000L)
        .keyBy(_.txId)

    // join处理
    val processedStream = orderEventStream.intervalJoin( receiptEventStream )
      .between(Time.seconds(-30), Time.seconds(30))
      .process( new TxPayMatchByJoin() )

    processedStream.print()

    env.execute("tx pay match by join job")
  }
}
class TxPayMatchByJoin extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect((left, right))
  }
}