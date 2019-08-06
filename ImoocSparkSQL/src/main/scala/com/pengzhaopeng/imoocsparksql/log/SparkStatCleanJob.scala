package com.pengzhaopeng.imoocsparksql.log

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  * 使用Spark完成我们的数据清洗操作 呵呵
  */
object SparkStatCleanJob {

  def main(args: Array[String]) {
    val spark: SparkSession = SparkSession.builder().appName("SparkStatCleanJob")
      .config("spark.sql.parquet.compression.codec", "gzip")
//      .config("spark.sql.sources.partitionColumnTypeInference.enabled", value = false)
      .master("local[2]").getOrCreate()

    val accessRDD: RDD[String] = spark.sparkContext.textFile("D:/test/input/10000_access.log")

    //accessRDD.take(10).foreach(println)
    import spark.implicits._
    val accessDF: DataFrame = accessRDD.map { line =>
      val splits: Array[String] = line.split(" ")
      val ip = splits(0)
      val url = splits(1)
      val traffic: Long = splits(2).toLong

      val domain = "http://www.imooc.com/"
      val cms: String = url.substring(url.indexOf(domain) + domain.length)
      val cmsTypeId: Array[String] = cms.split("/")

      var cmsType = ""
      var cmsId = 0l
      if (cmsTypeId.length > 1) {
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1).toLong
      }

      val city: String = IpUtils.getCity(ip)
      val time = splits(0)
      val day: String = time.substring(0, 10).replaceAll("-", "")
      (url, cmsType, cmsId, traffic, ip, city, time, day)

    }.toDF("url", "cmsType", "cmsId", "traffic", "ip", "city", "time", "day")


    //RDD ==> DF
    //    val accessDF: DataFrame = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
    //      AccessConvertUtil.struct)

    accessDF.printSchema()
    accessDF.show(false)

    //    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
    //      .partitionBy("day").save("D:/test/output")

    spark.stop
  }
}
