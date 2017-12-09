package com.zhiyou.bd14.common

import java.nio.charset.Charset

import org.apache.flume.api.RpcClientFactory
import org.apache.flume.event.EventBuilder

class FlumeClient(hostname:String, port:Int) {



  val client = RpcClientFactory.getDefaultInstance(hostname, port)



  def sendDataToFlume(randomData:String) = {
    val event = EventBuilder.withBody(randomData, Charset.forName("UTF-8"))
    client.append(event)
    Thread.sleep(300)
//    println(s"sended: $randomData")
  }






  def close() = {
    client.close()
  }


  object FlumeClient {
    def apply(hostname:String, port:Int):FlumeClient =
      new FlumeClient(hostname,port)
  }

}
