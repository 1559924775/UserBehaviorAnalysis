
import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random
case class MarketingUserBehavior(userId:String,behavior:String,channel:String,timestamp:Long)

/**
 * 统计app对的行为以及获取app的渠道
 */
object AppMarketingByChannel {
  def main(args:Array[String]):Unit={
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //自定义数据源
    env.addSource(new SimulatedAppMarketingSource)
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior!="uninstall")
      .map(data=>((data.behavior,data.channel),1L))
      .keyBy(_._1)
      .timeWindow(Time.hours(1),Time.seconds(10))
      .process(new AppMarketingByChannelProcessFunction)
      .print()

    env.execute("app promoton analysis job")
  }
}
class SimulatedAppMarketingSource extends RichSourceFunction[MarketingUserBehavior]{
  var runing=true
  val rand=new Random()
  lazy val behaviorTypes:Seq[String] = Seq("clink","clink","clink","clink","clink","clink","download","install","download","install","unistall")
 lazy val channelSets:Seq[String]=Seq("wechat","baidu App","appstore","weibo")
  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    val maxCount=Long.MaxValue
    var count=0L
    while(runing&&count<maxCount){
      val id=UUID.randomUUID().toString
      val behavior=behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel=channelSets(rand.nextInt(channelSets.size))
      val ts=System.currentTimeMillis()
      ctx.collect(MarketingUserBehavior(id,behavior,channel,ts))
      count+=1
      TimeUnit.MILLISECONDS.sleep(10L)//休息10毫秒
    }
  }

  override def cancel(): Unit = runing=false
}

class AppMarketingByChannelProcessFunction extends ProcessWindowFunction[((String,String),Long),String,(String,String),TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[String]): Unit = {
    val count=elements.size
    val startTime=context.window.getStart
    val endTime=context.window.getEnd
    val result=new StringBuilder
    result.append("时间：").append(new Timestamp(startTime)).append(" -- ").append(new Timestamp(endTime))
      .append("\n")
      .append("行为：").append(key._1)
      .append("\t渠道：").append(key._2)
      .append("\t计数：").append(count)
      .append("\n")
      .append("===================================================================")
    out.collect(result.toString())
  }
}