package com.zhiyou.warehouse.sparksql

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession


/*
*
* 源日志数据：
    1.timestamp 行为时间戳
    2.user_id   用户标识
    3.session_id 用户一次登录行为的session标识
    4.page_id  用户本次行为相关的页面标识（和产品、品类、关键词相关）
    5.action_type 1.关键词搜索，2.浏览产品，3.加入购物车,4.下单，5.支付
    6.关键词内容
    7.相关产品标识
    8.相关产品品类标识
*
* */
object ActionLogSparkSql {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("spark read hdfs")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._
  import spark.sql
  def readActionLogFromHdfs() = {
    //spark读取hdfs上的文件, 生成DataFrame格式数据
    val df = spark.read.text("/user/ECPro/ActionLog/20171207")
    df.printSchema()

    //对ds进行字符串, 划分, 生成Dataset格式文件, dataset格式可以创建视图
    val ds = df.map(x => {
      val info = x.getString(0).split("\\|")
      (info(0),info(1),info(2),info(3),info(4),
      info(5),info(6),info(7))
    })

    ds.printSchema()
    ds.createOrReplaceTempView("action_log")


    //源日志数据的内容
    /*
    *
    * 源日志数据：
    1.timestamp 行为时间戳
    2.user_id   用户标识
    3.session_id 用户一次登录行为的session标识
    4.page_id  用户本次行为相关的页面标识（和产品、品类、关键词相关）
    5.action_type 1.关键词搜索，2.浏览产品，3.加入购物车,4.下单，5.支付
    6.关键词内容 content
    7.相关产品标识 product_id
    8.相关产品品类标识 category_id
    * */


    //自定义一个函数把2014-02-22 00:00:00 日期转换成2014-20-22格式
    //Tue Jul 18 00:00:17 GMT+08:00 2017
    spark.udf.register("my_dt_to_date"
      , (dateTime:String) => {
          //Timestamp类型
          val dateTime1 = new Timestamp(dateTime.toLong)
          dateTime1.toString
      })


    /*
      * 宽表数据：
        1.date  行为日期
        2.user_id
        3.session_id
        4.page_id
        5.action_time
        6.search_keyword  搜索关键词
        7.click_category_id  浏览的产品品类标识
        8.click_product_id   浏览的产品标识
        9.order_category_ids   下单的产品品类标识
        10.order_product_ids   下单的产品标识
        11.pay_category_ids    支付的产品品类标识
        12.pay_product_ids     支付的产品标识
    * */

    //首先判断action_type   1.关键词搜索，2.浏览产品，3.加入购物车,4.下单，5.支付 随机
    val result = sql(
      """
        |select my_dt_to_date(_1) timestamp
        |       , _1
        |       , _2 user_id
        |       , _3 session_id
        |
        |       , _4 page_id
        |       , _5 action_type
        |       , _6 content
        |       , _7 product_id
        |       , _8 category_id
        |from action_log
      """.stripMargin
    ).orderBy("timestamp").orderBy("user_id")

    println("action_log表")

    result.printSchema()
    result.show()

  }


  def readAdvClickFromHdfs() = {
    //spark读取hdfs上的文件, 生成DataFrame格式数据
    val df = spark.read.text("/user/ECPro/AdvClick/20171207")
    df.printSchema()

    //对ds进行字符串, 划分, 生成Dataset格式文件, dataset格式可以创建视图
    val ds = df.map(x => {
      val info = x.getString(0).split("\\|")
      (info(0),info(1),info(2),info(3),info(4))
    })

    ds.printSchema()
    ds.createOrReplaceTempView("advclick_log")


    //源日志数据的内容
    /*
    *
    * **********************

      需要连接adcClick表
      1512642416746|100001|1000017|河南|郑州
      1512642418439|100004|1000012|湖南|长沙

      1512642416746|	timestamp
      100001|		      user_id
      1000017|      	adid
      河南		          province
      郑州		          city

    **********************
    * */
    val result = sql(
      """
        |select _1 timestamp
        |       , _2 user_id
        |       , _3 adid
        |       , _4 province
        |       , _5 city
        |from advclick_log
      """.stripMargin
    )
    println("advclick_log表")
    result.show()

  }





  def main(args: Array[String]): Unit = {
    readActionLogFromHdfs()
//    readAdvClickFromHdfs()


  }




















}
