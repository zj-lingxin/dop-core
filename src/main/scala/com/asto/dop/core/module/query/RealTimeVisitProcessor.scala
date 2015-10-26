package com.asto.dop.core.module.query

import com.asto.dop.core.entity.VisitEntity
import com.ecfront.common.Resp

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Promise

/**
 * 实时访客
 */
object RealTimeVisitProcessor extends QueryProcessor {

  override protected def process(req: Map[String, String], p: Promise[Resp[Any]]): Unit = {
    if (!req.contains("source") || !req.contains("platform") || !req.contains("pageNumber")) {
      p.success(Resp.badRequest("【source】【platform】【pageNumber】不能为空"))
    } else {
      val source = req("source")
      val platform = req("platform")
      val pageNumber = req("pageNumber").toLong
      val pageSize = req.getOrElse("pageSize", "15").toInt
      VisitEntity.db.page("v_source =?  AND c_platform =? ", List(source, platform), pageNumber, pageSize).onSuccess {
        case pageResp =>
          p.success(Resp.success(pageResp.body))
      }
    }
  }

}
