package com.asto.dop.core.module.query

import com.asto.dop.core.entity.VisitEntity
import com.asto.dop.core.helper.DBHelper
import com.ecfront.common.{JsonHelper, Resp}
import io.vertx.core.json.JsonObject
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

/**
 * 实时访客
 */
object RealTimeVisitProcessor extends QueryProcessor {

  def visitorDetails(req: Map[String, String]): Future[Resp[Any]] = {
    val p = Promise[Resp[Any]]
    if (!req.contains("visitor_id")) {
      Future(Resp.badRequest("【visitor_id】不能为空"))
    } else {
      val visitorId = req("visitor_id")

      //访问时间，访问页面和停留时间
      val visitTimeAndUrlResp = DBHelper.find(s"SELECT occur_time,v_url,v_residence_time FROM ${VisitEntity.db.TABLE_NAME} WHERE visitor_id = ?  ORDER BY occur_time DESC", List(visitorId), classOf[JsonObject])

      for {
        visitTimeAndUrl <- visitTimeAndUrlResp
      } yield {
        val result = visitTimeAndUrl.body.map{
          i =>
            s"""
               |{
               |    "occur_time":${i.getLong("occur_time")},
               |    "v_url":"${i.getString("v_url")}",
               |    "v_residence_time":${i.getLong("v_residence_time")}
               |}
               |""".stripMargin
        }.mkString("[",",","]")
        p.success(Resp.success(JsonHelper.toJson(result)))
      }
    }
    p.future
  }


  override protected def process(req: Map[String, String], p: Promise[Resp[Any]]): Unit = {
    if (!req.contains("pageNumber")) {
      p.success(Resp.badRequest("【pageNumber】不能为空"))
    } else {
      var sqlSeg = ""
      val parameters = ArrayBuffer[Any]()
      if (req.contains("source")) {
        sqlSeg = s" AND v_source = ? "
        parameters += req("source")
      }
      if (req.contains("platform")) {
        sqlSeg += s" AND c_platform = ?  "
        parameters += req("platform")
      }
      val pageNumber = req("pageNumber").toLong
      val pageSize = req.getOrElse("pageSize", "15").toInt
      VisitEntity.db.page(s" 1=1 $sqlSeg ", parameters.toList, pageNumber, pageSize).onSuccess {
        case pageResp =>
          p.success(Resp.success(pageResp.body))
      }
    }
  }

}
