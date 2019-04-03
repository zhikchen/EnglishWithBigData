package cn.jxufe.czk.action

import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}

import scala.collection.mutable.ArrayBuffer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}


//单例对象
object createKafkaProducerPool{

  //用于返回真正的对象池 --> GenericObjectPool
  def apply(brokerList: String, topic: String):  GenericObjectPool[KafkaProducerProxy] = {
    val producerFactory = new BaseKafkaProducerFactory(brokerList, defaultTopic = Option(topic))
    val pooledProducerFactory = new PooledKafkaProducerAppFactory(producerFactory)
    //指定了kafka对象池的大小
    val poolConfig = {
      val c = new GenericObjectPoolConfig
      val maxNumProducers = 10
      c.setMaxTotal(maxNumProducers)
      c.setMaxIdle(maxNumProducers)
      c
    }
    //返回一个对象池
    new GenericObjectPool[KafkaProducerProxy](pooledProducerFactory, poolConfig)
  }
}

object KafkaStreaming{

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[4]").setAppName("two-tuples")
    val ssc = new StreamingContext(conf, Seconds(1))

    //创建topic
    val brobrokers = "master:9092,slave1:9092,slave2:9092"
    val sourcetopic="first";
    val targettopic = "second"


    //创建消费者组
    var group="con-consumer-group"
    //消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> brobrokers,//用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> group,
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    );

    //ssc.sparkContext.broadcast(pool)

    //创建DStream，返回接收到的输入数据
    var stream=KafkaUtils.createDirectStream[String,String](ssc, LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array(sourcetopic),kafkaParam))
    val result = stream.flatMap[String](s => {
        var list = ArrayBuffer[String]()
        var split = s.value().split("[\"|,|\" \"|(|)|:|'|.|!|?]")
        for(i <- 1 to split.length-1){
          val result = split(i - 1).trim + "," + split(i).trim
          if (!result.endsWith(",") && !result.startsWith(","))
            list += result
        }
      list
    }).map((_,1)).reduceByKey(_+_)

    val updateFunc = (values : Seq[Int],state:Option[Int]) => {
      val newValue = values.foldLeft(0)(_+_)
      val  oldValue= state.getOrElse(0)
      Some( oldValue + newValue )
    }
    ssc.checkpoint("hdfs://master:9000/checkpoint")
    val state = result.updateStateByKey[Int](updateFunc)

    state.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        // kafka连接池
        val pool = createKafkaProducerPool(brobrokers, targettopic)
        //从连接池中取出一个kafka连接
        val p = pool.borrowObject()
        partitionOfRecords.foreach {message => System.out.println(message._2);p.send(message._1,String.valueOf(message._2),Option(targettopic))}
        //使用完连接之后，放回去
        pool.returnObject(p)

      })
    })

    ssc.start()
    ssc.awaitTermination()

  }
}