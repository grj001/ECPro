package com.zhiyou.bd14.collector.log

import com.zhiyou.bd14.common.FlumeClient

import scala.util.Random

/**
  * timestamp	1450702800
  * province 	Jiangsu
  * city 	Nanjing
  * userid 	100001
  * adid 	100001
  */
object AdvClick {
  val provinces = Array("河南","河北","云南","湖南")
  val citys = Map("河南"->Array("郑州","洛阳","安阳"),
              "河北"->Array("石家庄","秦皇岛"),
              "云南"->Array("昆明","大理","丽江"),
              "湖南"->Array("长沙","襄阳","湘潭","娄底")
  )


  val random = new Random()
  def main(args: Array[String]): Unit = {
    val flumeClient = new FlumeClient("master",8888)
    while(true){
      val randomData = mockData()
      flumeClient.sendDataToFlume(randomData)
      Thread.sleep(500 + random.nextInt(200))
    }
  }




  def mockData():String = {
    val timestamp = System.currentTimeMillis()
    //20个用户随机
    val userId = "10000" + random.nextInt(20)
    val adid = "10000" + random.nextInt(20)
    val province = provinces(random.nextInt(4))
    val cityArray = citys(province)
    val city = cityArray(random.nextInt(cityArray.length))
    s"$timestamp|$userId|$adid|$province|$city"
  }









}
