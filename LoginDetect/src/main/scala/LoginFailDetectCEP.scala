import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern

import scala.collection.Map

object LoginFailDetectCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[LoginEvent] = env.readTextFile("D:\\IDEA\\UserBehaviorAnalysis\\LoginDetect\\src\\main\\resources\\LoginLog.csv")
      .map(data => {
        val arr = data.split(",")
        LoginEvent(arr(0).trim.toLong, arr(1).trim, arr(2).trim, arr(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(6)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime
      }) //数据有点乱序,给个6秒的延迟

    val userIdKeyedStream: KeyedStream[LoginEvent, Long] = dataStream.keyBy(_.userId)

    val selectPattern:Pattern[LoginEvent,LoginEvent] = Pattern.begin[LoginEvent]("start")
      .where(_.eventType=="fail")
      .next("end")
      .where(_.eventType=="fail").within(Time.seconds(2))
    val patternStream:PatternStream[LoginEvent] = CEP.pattern(userIdKeyedStream, selectPattern)
    //获取匹配
    val loginFailStream = patternStream.select((map:Map[String, Iterable[LoginEvent]]) => {
      val first: LoginEvent = map.getOrElse("start",null).iterator.next()
      val next = map.getOrElse("end",null).iterator.next()
      Warning(first.userId, first.eventTime, next.eventTime, next.eventType)
    })
    loginFailStream.print()
    env.execute("login check job")

  }
}
