package com.uestc

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector
import TxMatchCheck._
/**
 * 实时对账系统，对比支付日志和平台的交易日志
 */
case class OrderEvent( orderId: Long, eventType: String, txId: String, eventTime: Long )
case class ReceiptEvent( txId: String, payChannel: String, eventTime: Long )

object TxMatchCheck {
  //定义两个测输出标签
  val unmatchedPays:OutputTag[OrderEvent] = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts=new OutputTag[ReceiptEvent]("unmatchedReceipts")
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
//    env.socketTextStream("180.76.169.198",7779)
    .map( data => {
      val dataArray = data.split(",")
      ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
    })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    val processStream:DataStream[(OrderEvent,ReceiptEvent)] = orderEventStream.connect(receiptEventStream)
      .process(new MatchProcess)

    processStream.print()
    processStream.getSideOutput(unmatchedPays).print("unmatchedPays")
    processStream.getSideOutput(unmatchedReceipts).print("unmatchedReceipts")
    env.execute("tx match job")

  }
}
/************************************
//process方法和watermark没关系，来一条处理一条（超过watermark的数据也不会被丢弃，但是watermark5s后会触发定时器删除数据）。
定时器才要等到watermark涨上来（多条流木桶原理）。
***********************************************/

class MatchProcess extends CoProcessFunction[OrderEvent,ReceiptEvent,(OrderEvent, ReceiptEvent)]{
  lazy val payState:ValueState[OrderEvent]=getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("payState",classOf[OrderEvent]))
  lazy val receiptState:ValueState[ReceiptEvent]=getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("createState",classOf[ReceiptEvent]) )
  override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val receipt=receiptState.value()
    if(receipt!=null){
//      ctx.timerService().deleteEventTimeTimer(receipt.eventTime*1000+5000L)
      out.collect((pay,receipt))//账对上了
      receiptState.clear()
    }else{
      payState.update(pay)
      //等5s再看看
      ctx.timerService().registerEventTimeTimer(pay.eventTime*1000+5000L)
    }
  }

  override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val pay=payState.value()
    if(pay!=null){
//      ctx.timerService().deleteEventTimeTimer(pay.eventTime*1000+5000L) //来了不要再定时看了
      out.collect((pay,receipt))//账对上了
      payState.clear()
    }else {
      receiptState.update(receipt)
      ctx.timerService().registerEventTimeTimer(receipt.eventTime*1000+5000L)
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
//    //首先不可能两个都没来，因为是按tx_id做keyBy的
//    //如果两个都来了，要么不会注册定时器，要么定时器被删了   （删定时器可能删错，因为是按照时间来删的）
//    //所以只能是一个来了，另一个没来
//    //1.receipt没来
//    if(receiptState.value()==null){
//      ctx.output(unmatchedPays,payState.value())
//      payState.clear()
//    }
//    //2.pay没来
//    if(payState.value()==null) {
//      println("============================================================="+receiptState.value())
//      ctx.output(unmatchedReceipts,receiptState.value())
//      receiptState.clear()
//    }

    // 到时间了，如果还没有收到某个事件，那么输出报警信息
    if( payState.value() != null ){
      // recipt没来，输出pay到侧输出流
      ctx.output(unmatchedPays, payState.value())
    }
    if( receiptState.value() != null ){
      ctx.output(unmatchedReceipts, receiptState.value())
    }
    payState.clear()
    receiptState.clear()
  }
}