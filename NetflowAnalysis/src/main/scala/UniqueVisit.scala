
import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable
case class Vistor(userId:Long,behavior: String,count:Long,timestamp:Long)

/**
 * 每隔1小时统计网站的UV，使用内存空间去重
 */
object UniqueVisit {
  def main(args:Array[String]):Unit={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream:DataStream[Vistor] = env.readTextFile("D:\\IDEA\\UserBehaviorAnalysis\\NetflowAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(data => {
        val arr = data.split(",")
        Vistor(arr(0).trim.toLong, arr(3).trim, 1L, arr(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp*1000)
      .filter(_.behavior=="pv")

    val applyStream:DataStream[String] = dataStream
      .timeWindowAll(Time.hours(1))
      .apply(new UniqueVisitWindowFunction)

    applyStream.print()

    env.execute("unique visit job")
  }
}

class UniqueVisitWindowFunction extends AllWindowFunction[Vistor,String,TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[Vistor], out: Collector[String]): Unit = {
    //使用set对用户进行去重   -- 问题是这将撑爆内存
    val set:mutable.Set[Long] = mutable.Set[Long]()
    val iterator = input.iterator
    while(iterator.hasNext){
      var userId=iterator.next().userId
      set+=userId
    }
    val count=set.size
    val result = new StringBuilder
    result.append("时间：").append(new Timestamp(window.getStart)).append(" -- ")
      .append(new Timestamp(window.getEnd)).append("\n")
      .append("UV:").append(count).append("\n")
      .append("=========================================")
    out.collect(result.toString())
  }
}