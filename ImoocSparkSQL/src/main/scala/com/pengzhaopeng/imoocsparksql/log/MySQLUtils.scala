package com.pengzhaopeng.imoocsparksql.log

import java.sql.{Connection, PreparedStatement, DriverManager}

/**
 * MySQL操作工具类
 */
object MySQLUtils {

  /**
   * 获取数据库连接
   */
  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://hadoop01:3306/imooc_project?useUnicode=true&characterEncoding=utf-8&user=root&password=123456aa")
  }

  /**
   * 释放数据库连接等资源
   * @param connection
   * @param pstmt
   */
  def release(connection: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }

  def main(args: Array[String]) {
    println(getConnection())
  }

}
