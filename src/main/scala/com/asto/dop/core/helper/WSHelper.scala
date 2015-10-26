package com.asto.dop.core.helper

import com.ecfront.common.JsonHelper
import com.typesafe.scalalogging.slf4j.LazyLogging
import io.vertx.core.Handler
import io.vertx.core.http._

import scala.collection.mutable.ListBuffer

/**
 * Websocket 异步操作辅助类
 *
 */
object WSHelper extends LazyLogging {

  private val websockets = ListBuffer[ServerWebSocket]()

  def createWS(websocket: ServerWebSocket): Unit = {
    websocket.closeHandler(new Handler[Void] {
      override def handle(event: Void): Unit = {
        websockets -= websocket
      }
    })
    websockets += websocket
  }

  def ws(data: WSReq) = {
    websockets.foreach(_.writeFinalTextFrame(JsonHelper.toJsonString(data)))
  }

}


case class WSReq(action: String, body: Any)

