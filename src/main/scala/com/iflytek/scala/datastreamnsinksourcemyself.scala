package com.iflytek.scala

import com.iflytek.scala.streamsource.{MyNoParallelSource, MyRichParallelSource}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object datastreamnsinksourcemyself {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._

    val text = env.addSource(new MyRichParallelSource)
//    val text = env.addSource(new MyNoParallelSource)

    val mapData = text.map( line => {
      println("接收到的数据："+line)
      line
    })
    val sum = mapData.timeWindowAll(Time.seconds(2))
      .sum(0)

    sum.print().setParallelism(1)

    val conf = new FlinkJedisPoolConfig
      .Builder()
      .setHost("master")
      .setPort(6379)
      .build()
    val redisSink = new RedisSink[Tuple2[String,String]](conf,new MyRedisMapper)
    sum.map(x=>(x.toString,"test")).addSink(redisSink)

    env.execute("StreamingDemoWithMyNoParallelSourceScala")
  }

}

class MyRedisMapper extends RedisMapper[Tuple2[String,String]]{

  override def getKeyFromData(data: (String, String)) = {
    data._1
  }
  override def getValueFromData(data: (String, String)) = {
    data._2
  }
  override def getCommandDescription = {
    new RedisCommandDescription(RedisCommand.LPUSH)   // 具体操作命令
  }
}