
import java.sql.Timestamp

import AdStatisticsByGeo.blackListOutputTag
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
//输入广告点击时间样例类
case class AdClickEvent(userId:Long,adId:Long,province:String,city:String,timestamp:Long)
//按照省份统计的输出结果样例类
case class CountByProvince(windowEnd:String,province:String,count:Long)
// 输出的黑名单报警信息
case class BlackListWarning( userId: Long, adId: Long, msg: String )

/**
 * 带黑名单的广告点击统计（根据地域聚合）
 */
object AdStatisticsByGeo {
  // 定义侧输出流的tag
  val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList")
  def main(args:Array[String]):Unit={

    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream:DataStream[AdClickEvent] = env.readTextFile("D:\\IDEA\\UserBehaviorAnalysis\\MarketPromotionAnalysis\\src\\main\\resources\\AdClickLog.csv")
      .map(data => {
        val arr = data.split(",")
        AdClickEvent(arr(0).trim.toLong, arr(1).trim.toLong, arr(2).trim, arr(3).trim, arr(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)

    //过滤掉恶意点击的用户
    val filterStream:DataStream[AdClickEvent] = dataStream.keyBy(data => (data.userId, data.adId))
      .process(new FilterKeyedProcessFunction)

    val processStream:DataStream[String] = filterStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AggFunction, new AdWindowFunction)
      //美化一下输出，每个时间统计窗口中按点击量排序
        .keyBy(_.windowEnd)
        .process(new AdKeyedProcessFunction)

    filterStream.getSideOutput(blackListOutputTag).print("blacklist")
    processStream.print()

    env.execute("ad statistic job")
  }
}
class AggFunction extends AggregateFunction[AdClickEvent,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickEvent, accumulator: Long): Long = accumulator+1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a+b
}
class AdWindowFunction extends WindowFunction[Long,CountByProvince,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince(window.getEnd.toString,key,input.iterator.next()))
  }
}
class AdKeyedProcessFunction extends KeyedProcessFunction[String,CountByProvince,String]{
  //虽然不能获得一个窗口中相同key的所有元素，但可以用listState把它们存起来定时处理
  lazy val listState:ListState[CountByProvince] =getRuntimeContext.getListState(new ListStateDescriptor[CountByProvince]("listState",classOf[CountByProvince]))
  override def processElement(value: CountByProvince, ctx: KeyedProcessFunction[String, CountByProvince, String]#Context, out: Collector[String]): Unit = {
    listState.add(value)
    //通过windowEndTime聚合起来，用有相同的结束时间
    ctx.timerService().registerEventTimeTimer(value.windowEnd.toLong+1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, CountByProvince, String]#OnTimerContext, out: Collector[String]): Unit = {
    val listBuffer: ListBuffer[CountByProvince] = new ListBuffer
    import scala.collection.JavaConversions._
    for (item <- listState.get()) {
      listBuffer.add(item)
    }
    //顺便排一下序
    val sortItems = listBuffer.sortBy(_.count)(Ordering.Long.reverse)
    listState.clear()

    //格式化输出
    val result: StringBuilder = new StringBuilder
    result.append("时间：").append(new Timestamp(timestamp-1)).append("\n")
    for (i <- sortItems.indices) {
      var currCountByProvince = sortItems(i)
      result.append("NO.").append(i + 1).append("：")
        .append("\t省份：").append(currCountByProvince.province)
        .append("\t点击量：").append(currCountByProvince.count)
        .append("\n")
    }
    result.append("=============================================================")
    out.collect(result.toString())
  }
}

class FilterKeyedProcessFunction extends KeyedProcessFunction[(Long,Long),AdClickEvent,AdClickEvent]{
  lazy val countState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("countState",classOf[Long]))
  lazy val isOnBlankList:ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isOnBlankList",classOf[Boolean]))
  private val maxCount=100L
//  lazy val resetTimer: ValueState[Long] = getRuntimeContext.getState( new ValueStateDescriptor[Long]("resettimeState", classOf[Long]) )
  override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {

    val count=countState.value()
    if(count==0){
      //定时第二天0时清楚黑名单状态
//      val ts = ( ctx.timerService().currentProcessingTime()/(1000*60*60*24) + 1) * (1000*60*60*24) //第二天0时
      val ts=ctx.timerService().currentProcessingTime()+(24*60*60*1000) //加上一天的毫秒数，到第二天同一时刻
      ctx.timerService().registerProcessingTimeTimer(ts)
    }
    if(count>=maxCount){
      if(!isOnBlankList.value()){
        isOnBlankList.update(true)
        //警告信息输出到侧输出流
        ctx.output( blackListOutputTag, BlackListWarning(value.userId, value.adId, "Click over " + maxCount + " times today.") )
      }
      //恶意点击，该条数据直接过滤掉
      return
    }
    //放行
    countState.update(count+1)
    out.collect(value)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
    countState.clear()
    isOnBlankList.clear()
  }
}