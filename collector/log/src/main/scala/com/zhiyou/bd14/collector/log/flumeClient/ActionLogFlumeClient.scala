package com.zhiyou.bd14.collector.log.flumeClient

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.UUID

import com.zhiyou.bd14.common.FlumeClient

import scala.util.Random

object ActionLogFlumeClient {




  val random = new Random()
  // actiontime
  val dateFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val startTime = dateFormater.parse("2017-07-17 00:00:00").getTime
  val endTime = dateFormater.parse("2017-07-17 24:00:00").getTime
  val subTime = endTime - startTime
  //user_id   1-100之间随机
  //session_id uuid来表示
  //page_id   1-100之间随机
  //action_type 在1.关键词搜索，2.浏览产品，3.加入购物车,4.下单，5.支付 随机
  //key word内容，罗列一些关键词 然后随机 连衣裙 连衣裙女夏2017新款 男士外套 棒球服 外套男 运动外套 数据线 安卓 读卡器 安卓数据线
  //产品标识  1.女装下 1.连衣裙 2半身裙 3真丝裙
  //         2.男装下  4.外套 5.夹克 6.西装 7.风衣
  //           3.鞋靴    8.罗马凉鞋 9.夹趾拖 10.拖鞋
  //           4.箱包    11.单肩包 12.手提包 13.小方包 14.贝壳包
  //         5.童装玩具 15.童书 16户外玩具 17积木 18早教
  //         6.数码  19键盘 20显示器 21显卡 22鼠标 23苹果本
  //品类标识 1.女装，2.男装，3.鞋靴 4.箱包 5.童装玩具 6.数码
  val actionTypes = Array(1, 2, 3, 4, 5)
  val keyWords = Array("连衣裙", "连衣裙女夏2017新款", "男士外套", "棒球服", "外套男", "运动外套", "数据线", "安卓", "读卡器", "安卓数据线")
  val categorys = Array(1, 2, 3, 4, 5, 6)
  val products = Array(Array(1, 2, 3), Array(4, 5, 6, 7), Array(8, 9, 10), Array(11, 12, 13, 14), Array(15, 16, 17, 18), Array(19, 20, 21, 22, 23))


  def main(args: Array[String]): Unit = {

    (1 to 10).foreach(x => {
      oneSessionActionLog()
    })


  }


  def oneSessionActionLog() = {
    val userId = 1 + random.nextInt(100)
    val sessionId = UUID.randomUUID()
    var sessionStartTime = startTime + random.nextInt(subTime.toInt)
    //一次登录多次行为
    var recordCount = 0
    (1 to random.nextInt(100)).foreach(x => {
      val pageId = 1 + random.nextInt(100)
      val actionType = actionTypes(random.nextInt(5))
      val keyWord = if (actionType == 1) keyWords(random.nextInt(10)) else ""
      val categoryid = if (actionType != 1) categorys(random.nextInt(6)) else -1
      val productLength = if (categoryid != -1) products(categoryid - 1).length else -1
      val productid = if (productLength != -1) products(categoryid - 1)(random.nextInt(productLength)) else -1
      val acttionTime = if (x == 1) {
        sessionStartTime
      } else {
        sessionStartTime += 1000 + random.nextInt(60 * 1000)
        sessionStartTime
      }

      val flumeClient = new FlumeClient("master", 9999)
      flumeClient.sendDataToFlume(s"""$acttionTime|$userId|$sessionId|$pageId|$actionType|$keyWord|$categoryid|$productid""")

      //      out.println()
      recordCount += 1
    })
    println(recordCount)
  }
}
