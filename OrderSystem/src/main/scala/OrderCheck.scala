

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map


case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)
case class OrderResult(orderId: Long, eventType: String)

/**
 * 订单支付检测系统（订单在15分钟内未支付就关闭）
 */
object OrderCheck {
  def main(args:Array[String]):Unit={
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream:DataStream[OrderEvent] = env.readTextFile("D:\\IDEA\\UserBehaviorAnalysis\\OrderSystem\\src\\main\\resources\\OrderLog.csv")
      .map(data => {
        val arr = data.split(",")
        OrderEvent(arr(0).trim.toLong, arr(1).trim, arr(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(5)) {
        override def extractTimestamp(element: OrderEvent): Long = {
          element.eventTime
        }
      })

    val keyedStream:KeyedStream[OrderEvent,Long] = dataStream.keyBy(_.orderId)

    val pattern:Pattern[OrderEvent,OrderEvent] = Pattern.begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    val patternStream:PatternStream[OrderEvent] = CEP.pattern(keyedStream, pattern)

    val orderTimeOut:OutputTag[OrderResult]=new OutputTag[OrderResult]("orderTimeOut")
    val orderResult:DataStream[OrderResult] = patternStream.select(orderTimeOut) { //patternTimeoutFunction
      (map: Map[String, Iterable[OrderEvent]], Long) => {
        val create = map.getOrElse("create", null).iterator.next()
        OrderResult(create.orderId, "timeout")
      }
    } { //patternSelectFunction
      (map: Map[String, Iterable[OrderEvent]]) => {
        val pay = map.getOrElse("pay", null).iterator.next()
        OrderResult(pay.orderId, "success")
      }
    }

    val timeoutResult:DataStream[OrderResult] = orderResult.getSideOutput(orderTimeOut)

    orderResult.print("success==>")
    timeoutResult.print("timeout=====================>")
    env.execute("order check job")
  }
}
