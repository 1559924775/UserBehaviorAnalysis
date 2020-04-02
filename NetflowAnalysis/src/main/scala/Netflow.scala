


import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, StateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
// flatMap和Map需要引用的隐式转换
import org.apache.flink.api.scala._
case class Log(ip:String,timestamp: Long,method:String,url:String)
case class UrlViewCount(url:String,windowEnd:Long,count:Long)
/**
 * 实时热门页面统计  （每5秒，统计最近10分钟的）
 */
object Netflow {
  def main(args:Array[String]):Unit={
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream:DataStream[Log] = env.readTextFile("D:\\IDEA\\UserBehaviorAnalysis\\NetflowAnalysis\\src\\main\\resources\\apache.log")
      .map(data => {
        val arr = data.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        var timestamp = simpleDateFormat.parse(arr(3).trim).getTime //返回毫秒
        Log(arr(0).trim, timestamp, arr(5).trim, arr(6).trim)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Log](Time.seconds(1)) {
        override def extractTimestamp(element: Log): Long = {
          element.timestamp //已经是毫秒，无需*1000
        }
      })

    val processStream= dataStream
      .keyBy(_.url)
      //问题遗留：数据的延时太大怎么利用sidestream处理？？？
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.seconds(60))
      .aggregate(new AggFunction, new MyWindowFunction)
      .keyBy(_.windowEnd)
      .process(new TopNProcessFunction(5))

    processStream.print()
    env.execute("top url")

  }
}

class AggFunction extends AggregateFunction[Log,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: Log, accumulator: Long): Long = accumulator+1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a+b
}
class MyWindowFunction extends WindowFunction[Long,UrlViewCount,String,TimeWindow]{
//input是key window的输入，由于之前做了聚合，所以是count
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    val urlViewCount=UrlViewCount(key,window.getEnd,input.iterator.next())
    out.collect(urlViewCount)
  }
}

class TopNProcessFunction(n:Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{
  var listState:ListState[UrlViewCount]=_
  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    listState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd+1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit =
    {
      //数据转移到集合中
      val listBuffer = new ListBuffer[UrlViewCount]
      val iterator = listState.get().iterator()
      while (iterator.hasNext){
        listBuffer+=iterator.next()
      }
      listState.clear()
      //排序
      val sortedList = listBuffer.sortWith(_.count > _.count).take(n)
      //输出
      val result=new StringBuilder
      result.append("时间：").append(new Timestamp(timestamp-1)).append("\n")
      for(i <- sortedList.indices){
        result.append("NO.").append(i+1).append(": ")
          .append("热度：").append(sortedList(i).count)
          .append("\tURL：").append(sortedList(i).url)
          .append("\n")
      }
      result.append("============================================="+"\n")
      Thread.sleep(1000)
      out.collect(result.toString())
    }

  override def open(parameters: Configuration): Unit = {
    listState=getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("listState",classOf[UrlViewCount]) )
  }
}