package com.pengzhaopeng.imoocsparksql.log

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * TopN统计Spark作业：复用已有的数据
  */
object TopNStatJob2 {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("TopNStatJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .master("local[2]").getOrCreate()


    val accessDF: DataFrame = spark.read.format("parquet").load("D:/test/output")

    //    accessDF.printSchema()
    //    accessDF.show(false)

    val day = "20170511"

    import spark.implicits._
    //    val commonDF: Dataset[Row] = accessDF.filter($"day" === day && $"cmsType" === "video")
    val commonDF: Dataset[Row] = accessDF.filter($"cmsType" === "video")

    commonDF.cache()

    StatDAO.deleteData(day)

    //最受欢迎的TopN课程
    videoAccessTopNStat(spark, commonDF)

    //按照地市进行统计TopN课程
    cityAccessTopNStat(spark, commonDF)

    //按照流量进行统计
    videoTrafficsTopNStat(spark, commonDF)

    commonDF.unpersist(true)

    spark.stop()
  }

  /**
    * 按照流量进行统计
    */
  def videoTrafficsTopNStat(spark: SparkSession, commonDF: DataFrame): Unit = {
    import spark.implicits._

    //    val cityAccessTopNDF = commonDF.groupBy("day", "cmsId")
    //      .agg(sum("traffic").as("traffics"))
    //      .orderBy($"traffics".desc)
    //    //.show(false)

    commonDF.createOrReplaceTempView("video_traffic_top")
    val videoTrafficsTopNDF: DataFrame = spark.sql("select day,cmsId,sum(traffic) traffics from video_traffic_top " +
      "group by day, cmsId order by traffics desc")

    //    videoTrafficsTopNDF.show(false)

    /**
      * 将统计结果写入到MySQL中
      */
    try {
      videoTrafficsTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoTrafficsStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")
          list.append(DayVideoTrafficsStat(day, cmsId, traffics))
        })

        StatDAO.insertDayVideoTrafficsAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

  /**
    * 按照地市进行统计TopN课程
    */
  def cityAccessTopNStat(spark: SparkSession, commonDF: DataFrame): Unit = {

    val cityAccessTopNDF = commonDF
      .groupBy("day", "city", "cmsId")
      .agg(count("cmsId").as("times"))

    //cityAccessTopNDF.show(false)

    //Window函数在Spark SQL的使用

    val top3DF = cityAccessTopNDF.select(
      cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city"))
        .orderBy(cityAccessTopNDF("times").desc)
      ).as("times_rank")
    ).filter("times_rank <=3") //.show(false)  //Top3

    //    commonDF.createOrReplaceTempView("city_top_stat")
    //    val top3DF: DataFrame = spark.sql("select * from " +
    //      "(select day, city, cmsId,times, row_number() over(partition by city order by times desc) times_rank " +
    //      "from (select day,city,cmsId, count(1) as times from city_top_stat group by day, city,cmsId )t1 " +
    //      ")t2 where times_rank <= 3")

    top3DF.show(false)


    /**
      * 将统计结果写入到MySQL中
      */
    try {
      top3DF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayCityVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")
          list.append(DayCityVideoAccessStat(day, cmsId, city, times, timesRank))
        })

        StatDAO.insertDayCityVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }


  /**
    * 最受欢迎的TopN课程
    */
  def videoAccessTopNStat(spark: SparkSession, commonDF: DataFrame): Unit = {

    /**
      * 使用DataFrame的方式进行统计
      */
    import spark.implicits._

    //    val videoAccessTopNDF: Dataset[Row] = commonDF
    //      .groupBy("day", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)


    /**
      * 使用SQL的方式进行统计
      */
    val day = "20170511"
    val cmsType = "video"
    commonDF.createOrReplaceTempView("access_logs")
    //        val videoAccessTopNDF: DataFrame = spark.sql("select day,cmsId, count(1) as times from access_logs where day='" + day + "' and cmsType='" + cmsType + "' group by day, cmsId  order by times desc ")
    val videoAccessTopNDF: DataFrame = spark.sql("select day,cmsId, count(1) as times from access_logs where cmsType='" + cmsType + "' " +
      "group by day, cmsId  order by times desc ")

    //    videoAccessTopNDF.show(false)

    /**
      * 将统计结果写入到MySQL中
      */
    try {
      videoAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          /**
            * 不建议大家在此处进行数据库的数据插入
            */

          list.append(DayVideoAccessStat(day, cmsId, times))
        })

        StatDAO.insertDayVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

}
