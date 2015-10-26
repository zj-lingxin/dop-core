package com.asto.dop.core.module.collect

import java.net.URLDecoder

import com.asto.dop.core.entity.VisitEntity

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

/**
 * 心跳请求处理器，用于计算页面停留时间
 */
object HeartbeatProcessor extends VisitCollectProcessor {

  def process(req: Map[String, String]): Future[String] = {
    val p = Promise[String]()
    if (!req.contains("url") || !req.contains("request_id") || !req.contains("interval")) {
      p.success("【url】【request_id】【interval】不能为空")
    } else {
      val url = URLDecoder.decode(req("url"), "UTF-8")
      val requestId = req("request_id")
      val interval = req("interval").toInt
      val pvHash = getPVHash(url, requestId)
      VisitEntity.db.update("v_residence_time = v_residence_time+? ", "pv_hash = ? ", List(interval, pvHash)).onSuccess {
        case htResp =>
          if (htResp) {
            p.success("ok")
          } else {
            p.success(htResp.message)
          }
      }
    }
    p.future
  }

}
