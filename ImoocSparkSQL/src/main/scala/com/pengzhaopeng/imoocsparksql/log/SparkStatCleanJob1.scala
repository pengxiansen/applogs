package com.pengzhaopeng.imoocsparksql.log

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 使用Spark完成我们的数据清洗操作 呵呵
  */
object SparkStatCleanJob1 {

  def main(args: Array[String]) {

    if (args.length != 2) {
      println("Usage: SparkStatCleanJobYARN <inputPath> <outputPath>")
    }

    val Array(inputPath, outputPath) = args
//    val inputPath = "file:///D:/test/input/access*"
//    val outputPath = "file:///D:/test/output/"

    val spark: SparkSession = SparkSession
      .builder()
      .config("spark.sql.parquet.compression.codec", "gzip")
      .master("local[2]")
      .getOrCreate()

    val accessRDD: RDD[String] = spark.sparkContext.textFile(inputPath)

    //accessRDD.take(10).foreach(println)
    import spark.implicits._
    val accessDF: DataFrame = accessRDD.map { line =>
      val splits: Array[String] = line.split("\t")
      val time = splits(0)
      val day: String = time.substring(0, 10).replaceAll("-", "")
      val url: String = splits(1)
      val traffic: Long = splits(2).toLong
      val ip: String = splits(3)

      val domain = "http://www.imooc.com/"
      val cms: String = url.substring(domain.length)
      val cmsTypeId: Array[String] = cms.split("/")
      var cmsType = ""
      var cmsId = 0l
      if (cmsTypeId.length > 1) {
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1).toLong
      }

      val city: String = IpUtils.getCity(ip)

      (url, cmsType, cmsId, traffic, ip, city, time, day)

    }.toDF("url", "cmsType", "cmsId", "traffic", "ip", "city", "time", "day")


    //RDD ==> DF
    //    val accessDF: DataFrame = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
    //      AccessConvertUtil.struct)

    //    accessDF.printSchema()
    //    accessDF.show(false)

    accessDF
//      .coalesce(1)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .partitionBy("day")
      .save(outputPath)

    //停止
    spark.stop
  }
}
