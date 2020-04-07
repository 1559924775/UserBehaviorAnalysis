import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
case class LoginEvent(userId:Long,ip:String,eventType:String,eventTime:Long)
case class Warning(userId:Long,firstFailTime:Long,lastFailTime:Long,warningMsg:String)
/**
 * 检测用户恶意登录(2s秒内连续登录失败判为恶意登录）
 */
object LoginFailDetect {
  def main(args:Array[String]):Unit={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream:DataStream[LoginEvent] = env.readTextFile("D:\\IDEA\\UserBehaviorAnalysis\\LoginDetect\\src\\main\\resources\\LoginLog.csv")
      .map(data => {
        val arr = data.split(",")
        LoginEvent(arr(0).trim.toLong, arr(1).trim, arr(2).trim, arr(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(6)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime
      })//数据有点乱序

    val processStream:DataStream[Warning] = dataStream.keyBy(_.userId)
      .process(new LoginFailChechProcessFunction(2))

    processStream.print()

    env.execute("login check job")

  }
}
class LoginFailChechProcessFunction(retryTimes:Int) extends KeyedProcessFunction[Long,LoginEvent,Warning]{

  lazy val loginFailList = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginFailList",classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    //这种方式的缺点：有可能在2s内有大量的登录失败
    if(value.eventType=="fail"){
      //加入failList，注册定时器（第一次才注册）
      if(loginFailList.get().iterator().hasNext){
        ctx.timerService().registerEventTimeTimer(value.eventTime*1000+2000L)
      }
      loginFailList.add(value)
    }else if(value.eventType=="success"){
      //成功了就应该清除failList
      loginFailList.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    //距离第一次登录失败已经2s
    val listBuffer =new ListBuffer[LoginEvent]
    val failList = loginFailList.get().iterator()
    while (failList.hasNext){
      listBuffer+=failList.next()
    }
    if(listBuffer.size>=retryTimes){
      out.collect(Warning(listBuffer(0).userId,listBuffer(0).eventTime,listBuffer.last.eventTime,"retry times is "+listBuffer.size+" in 2 seconds"))
    }
    //清除failList
    loginFailList.clear()
  }
}
