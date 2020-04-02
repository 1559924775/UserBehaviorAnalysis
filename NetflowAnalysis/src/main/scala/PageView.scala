
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.functions.KeyedProcessFunction

import scala.collection.mutable.ListBuffer

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timeStamp: Long)

case class PVItem(pv: String, count: Long, windowEnd: Long)

/**
 * 每隔1小时统计网站的PV
 */
object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    val file=getClass.getResource("/UserBehavior.csv")
    //    println(file.getPath)
    val dataStream: DataStream[(String, Long)] = env.readTextFile("D:\\IDEA\\UserBehaviorAnalysis\\NetflowAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .filter(_.contains("pv"))
      .map(
        data => {
          val arr = data.split(",")
          UserBehavior(arr(0).trim.toLong, arr(1).trim.toLong, arr(2).trim.toInt, arr(3).trim, arr(4).trim.toLong)
        }
      )
      .assignAscendingTimestamps(_.timeStamp * 1000)
      .map(_ => ("pv", 1L))
    //    dataStream.print()
    val processStream: DataStream[String] = dataStream
      .timeWindowAll(Time.hours(1))
      .apply(new PageViewWindowFunction)
    processStream.print()




    //      .keyBy(_._1)
    //      //      .timeWindow(Time.minutes(10), Time.seconds(5))
    //      .timeWindow(Time.hours(1))
    //      .aggregate(new MyAggFunction, new PageViewMyWindowFunction)
    ////      .print()
    //      .map(data=>PVItem(data._1,data._2,data._3))
    //      .keyBy(_.windowEnd)
    //      .process(new MyKeyedProcessFunction)
    //      //      .sum(1)
    //    //      .print("page view")

    env.execute("page view job")
  }
}

class PageViewWindowFunction extends AllWindowFunction[(String, Long), String, TimeWindow] {
  //input就是整个窗口的输入，也就是整个窗口的数据
  override def apply(window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {
    var count: Int = input.size
    val result = new StringBuilder
    result.append("时间：").append(new Timestamp(window.getStart)).append(" -- ")
      .append(new Timestamp(window.getEnd)).append("\n")
      .append("PV:").append(count).append("\n")
      .append("=========================================")
    out.collect(result.toString())
  }
}


//class MyAggFunction extends AggregateFunction[(String, Long), Long, Long] {
//  override def createAccumulator(): Long = 0L
//
//  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1
//
//  override def getResult(accumulator: Long): Long = accumulator
//
//  override def merge(a: Long, b: Long): Long = a + b
//}
//
//class PageViewMyWindowFunction extends WindowFunction[Long, (String, Long, Long), String, TimeWindow] {
//  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[(String, Long, Long)]): Unit = {
//    var temp = input.iterator.next()
//    out.collect((key, temp, window.getEnd))
//  }
//
//  //  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[(String, Long, Long)]): Unit = ???
//}
//
//class MyKeyedProcessFunction extends KeyedProcessFunction[String, PVItem, String] {
//  private var count: ValueState[PVItem] = _
//
//  override def processElement(value: PVItem, ctx: KeyedProcessFunction[String, PVItem, String]#Context, out: Collector[String]): Unit = {
//    count.update(value)
//    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
//  }
//
//  override def open(parameters: Configuration): Unit = {
//    count = getRuntimeContext.getState(new ValueStateDescriptor[PVItem]("conunt", classOf[PVItem]))
//  }
//
//  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, PVItem, String]#OnTimerContext, out: Collector[String]): Unit = {
//    val result = new StringBuilder
//    result.append("时间").append(new Timestamp(timestamp - 1))
//      .append((count.value().pv, count.value().count))
//    count.clear()
//    out.collect(result.toString())
//  }
//}
