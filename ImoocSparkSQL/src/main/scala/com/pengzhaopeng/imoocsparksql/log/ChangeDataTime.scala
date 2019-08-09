package com.pengzhaopeng.imoocsparksql.log

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 将日志中2017-05-11 14:09:14 换成 2017-05-10 14:09:14
  */
object ChangeDataTime {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("ChangeDataTime").master("local[*]").getOrCreate()

    // access20170510.log
    // access20170514.log
    // access20170518.log
    val yymmdd = "2017-05-10 "
    val accessRDD: RDD[String] = spark.sparkContext.textFile("D:/test/input/access20170510.log")

    import spark.implicits._
    val accessDF: DataFrame = accessRDD.map { line =>
      val splits: Array[String] = line.split("\t")
      var date: String = splits(0)
      val hhMMss: String = date.split(" ")(1)
      date = yymmdd + hhMMss
      val url: String = splits(1)
      val traffic: Long = splits(2).toLong
      val ip: String = splits(3)
      (date, url, traffic, ip)
    }.toDF("date", "url", "traffic", "ip")

//    accessFilterRDD.saveAsTextFile("file:///D:/test/access20170510.log")

    accessDF
      .coalesce(1)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .partitionBy()
      .save("file:///D:/test/access20170510.log")

    spark.stop()

  }
}
