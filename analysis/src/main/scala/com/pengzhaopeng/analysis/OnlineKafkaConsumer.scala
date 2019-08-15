package com.pengzhaopeng.analysis

import java.io.InputStream
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.pengzhaopeng.comon.StartupReportLogs
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Table}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * 实时消费日志
  * 启动，跳转，错误日志
  */
object OnlineKafkaConsumer {

  //创建配置
  //创建消费者连接器
  //消费者迭代器，从消息流中获取数据

  def main(args: Array[String]): Unit = {
    val streamingContext: StreamingContext = StreamingContext.getOrCreate(loadProperties("streaming.checkpoint.path"), createContextFunc())

    streamingContext.start()
    //TODO优雅退出
    streamingContext.awaitTermination()
  }

  def createContextFunc(): () => StreamingContext = {
    () => {
      val kafkaBrokerList: String = loadProperties("kafka.broker.list")
      val kafkaGroup: String = loadProperties("kafka.group")
      val topic: String = loadProperties("kafka.topic")
      val interval: Long = loadProperties("streaming.interval").toLong

      //创建配置
      //创建SparkConf
      val sparkConf: SparkConf = new SparkConf().setAppName("online").setMaster("local[*]")

      // 优雅停止Spark
      // 暴力停掉sparkstreaming是有可能出现问题的，比如你的数据源是kafka，
      // 已经加载了一批数据到sparkstreaming中正在处理，如果中途停掉，
      // 这个批次的数据很有可能没有处理完，就被强制stop了，
      // 下次启动时候会重复消费或者部分数据丢失。
      sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
      // 每秒钟对于每个partition读取多少条数据
      // 如果不进行设置，Spark Streaming会一开始就读取partition中的所有数据到内存，给内存造成巨大压力
      // 设置此参数后可以很好地控制Spark Streaming读取的数据量，也可以说控制了读取的进度
      sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "100")
      // 指定Spark Streaming的序列化方式为Kryo方式
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 指定Kryo序列化的注册器
      sparkConf.set("spark.kryo.registrator", "com.pengzhaopeng.analysis.registrator.MyKryoRegistrator")

      //创建streamingContext
      val streamingContext = new StreamingContext(sparkConf, Seconds(interval))

      //启动checkpoint
      val checkPointPath: String = loadProperties("streaming.checkpoint.path")
      streamingContext.checkpoint(checkPointPath)

      //配置kafka的参数
      val kafkaParams: Map[String, Object] = Map[String, Object](
        "bootstrap.servers" -> kafkaBrokerList,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> kafkaGroup,
        "auto.offset.reset" -> "earliest", //earliest从头开始读- lastest-表示启动后产生的数据开始读
        "enable.auto.commit" -> (false: java.lang.Boolean) //禁用自动提交
      )

      //不止一个topic
      val topics: Array[String] = Array(topic)
      //创建消费者连接器
      val onlineLogDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
        streamingContext,
        //位置策略（如果kafka和spark程序部署在一起，会有最优位置）
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )
      //每隔10秒checkpoint一次
      onlineLogDStream.checkpoint(Duration(10000))

      //过滤垃圾数据
      //      val onlineFileredDStream: DStream[ConsumerRecord[String, String]] = onlineLogDStream.filter {
      //        message =>
      //          var success = true
      //
      //          val msgValue: String = message.value()
      //          if (!msgValue.contains("appVersion") && !msgValue.contains("currentPage") && !msgValue.contains("errorMajor")) {
      //            success = false
      //          }
      //
      //          //过滤脏数据
      //          if (msgValue.contains("appVersion")) {
      //            val startupReportLogs: StartupReportLogs = JSON.parseObject(msgValue, classOf[StartupReportLogs])
      //            if (startupReportLogs.getUserId == null || startupReportLogs.getAppId == null) {
      //              success = false
      //            }
      //          }
      //          success
      //      }

      //完成需求统计并写入HBase
      onlineLogDStream.foreachRDD { rdd =>
        if (!rdd.isEmpty()) {
          //获取该RDD对于的偏移量
          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

          //处理业务逻辑
          rdd.foreachPartition { item =>
            val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
            println(s"偏移量：${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")

            val table: Table = getHBaseTable(getProperties())

            while (item.hasNext) {
              val msgValue: String = item.next().value()
              //TODO 这里先只计算启动日志
              if (msgValue.contains("appVersion")) {
                val startupReportLogs: StartupReportLogs = JSON.parseObject(msgValue, classOf[StartupReportLogs])
                val date = new Date(startupReportLogs.getStartTimeInMs)
                val dateTime: String = dateToString(date)
                val rowKey: String = dateTime + "_" + startupReportLogs.getCity
                table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes("StatisticData"), Bytes.toBytes("userNum"), 1L)
                println(rowKey)
              }
            }
          }

          //更新偏移量
          onlineLogDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        }

      }

      streamingContext
    }
  }

  /**
    * 读取配置文件
    *
    * @return
    */
  def getProperties(): Properties = {
    val properties = new Properties()
    val in: InputStream = getClass.getClassLoader.getResourceAsStream("config.properties")
    properties.load(in);
    properties
  }

  /**
    * 读取配置文件属性
    *
    * @param key
    * @return
    */
  def loadProperties(key: String): String = {
    val properties = new Properties()
    val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream("config.properties")
    properties.load(inputStream)
    properties.getProperty(key)
  }

  def dateToString(date: Date): String = {
    val dateString = new SimpleDateFormat("yyyy-MM-dd")
    val dateStr: String = dateString.format(date)
    dateStr
  }

  def getHBaseTable(pro: Properties) = {
    // 创建HBase配置
    val config: Configuration = HBaseConfiguration.create
    // 设置HBase参数
    config.set("hbase.zookeeper.property.clientPort", loadProperties("hbase.zookeeper.property.clientPort"))
    config.set("hbase.zookeeper.quorum", loadProperties("hbase.zookeeper.quorum"))
    // 创建HBase连接
    val connection: Connection = ConnectionFactory.createConnection(config)
    // 获取HBaseTable
    val table: Table = connection.getTable(TableName.valueOf(loadProperties("hbase.table.online_city_click_count")))
    table
  }
}
