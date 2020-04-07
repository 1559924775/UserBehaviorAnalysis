
import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.triggers._
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

//case class Vistor(userId:Long,behavior: String,count:Long,timestamp:Long)
/**
 * 每隔1小时统计网站的UV，使用布隆过滤器去重
 */
import org.apache.flink.api.scala._
object UniqueVistBloom {
  def main(args:Array[String]):Unit={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataSteam:DataStream[Vistor] = env.readTextFile("D:\\IDEA\\UserBehaviorAnalysis\\NetflowAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(data => {
        val arr = data.split(",")
        Vistor(arr(0).trim.toLong, arr(3).trim, 1L, arr(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
//        .filter(_.behavior=="pv")

    val processStream:DataStream[String] = dataSteam.keyBy(_.behavior)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger) //自定义trigger，每一条数据都要触发，默认是一个窗口触发的
      .process(new BloomProcessWindowFunction)

    processStream.print()

    env.execute("unique vist job with bloom")

  }
}
class MyTrigger extends Trigger[Vistor,TimeWindow]{
  override def onElement(element: Vistor, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext) ={
  //每来一条数据直接触发
    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}
/**
@Override
public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
		if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
			// if the watermark is already past the window fire immediately
			return TriggerResult.FIRE;
		} else {
			ctx.registerEventTimeTimer(window.maxTimestamp());
			return TriggerResult.CONTINUE;
		}
	} // 逻辑：水位线到窗口末端了，就全部提交，否则注册定时到窗口末端触发

@Override    //本来是定时触发的（窗口末端触发）
public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
		return time == window.maxTimestamp() ?
			TriggerResult.FIRE :
			TriggerResult.CONTINUE;
	}

 */
class Bloom(size :Long) extends Serializable{
  //位图的总大小
  private val cap=if(size>0)size else 1<<27  //默认16M
  def hash(value:String,seed:Int):Long={
    var result=0L
    for(i<- 0 until value.length){
      result=result*seed+value.charAt(i)
    }
    result & (cap-1) //同result % cap
  }
}

class BloomProcessWindowFunction extends ProcessWindowFunction[Vistor,String,String,TimeWindow]{
//  lazy val jedis=new Jedis("180.76.169.198",6379)
lazy val jedis=new Jedis("192.168.1.128",6379)
  lazy val bloom=new Bloom(1<<29)//64M
  var endTime:String="null" //endTime变化了说明一个窗口处理完了
  //自定义trigger后，每一条数据都会被这个函数处理
  override def process(key: String, context: Context, elements: Iterable[Vistor], out: Collector[String]): Unit = {
      val storeKey=context.window.getEnd.toString
      var count=0L
      //把存的count拿出来
      if(jedis.hget("count",storeKey)!=null){
        count=jedis.hget("count",storeKey).toLong
      }
      val vistor=elements.iterator.next()

      //使用布隆过滤器来去重 ，用时间换空间
      val bloomUserId=bloom.hash(vistor.userId.toString,61)
      val isExist = jedis.getbit(storeKey,bloomUserId)
      if(isExist){//已经有了，重复
//        out.collect(("UV",count,storeKey).toString())
      }else{
        jedis.setbit(storeKey,bloomUserId,true)
        jedis.hset("count",storeKey,(count+1).toString)
//        out.collect(("UV",count+1,storeKey).toString())
      }
    if( !storeKey.equals(endTime)){
      if(!endTime.equals("null")){
        var preCount=jedis.hget("count",endTime).toLong
        //输出上一个窗口的数据
        out.collect(("UV",preCount,new Timestamp(endTime.toLong)).toString())
      }
      endTime=storeKey
    }
  }
}