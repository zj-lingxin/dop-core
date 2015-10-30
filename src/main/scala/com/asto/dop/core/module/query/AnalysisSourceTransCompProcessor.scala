package com.asto.dop.core.module.query

import com.asto.dop.core.entity.{UserOptEntity, VisitEntity}
import com.asto.dop.core.helper.DBHelper
import com.ecfront.common.Resp
import io.vertx.core.json.JsonObject

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

/**
 * 来源转化构成
 */
object AnalysisSourceTransCompProcessor extends QueryProcessor {

  def summaryProcess(req: Map[String, String]): Future[Resp[Any]] = {
    val p = Promise[Resp[Any]]
    if (!req.contains("start") || !req.contains("end")) {
      p.success(Resp.badRequest("【start】【end】不能为空"))
    } else {
      val start = req("start").toLong
      val end = req("end").toLong
      //访客数
      val visitorCountResp = DBHelper.find(s"SELECT v_source ,COUNT(DISTINCT visitor_id) count FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? GROUP BY v_source", List(start, end), classOf[JsonObject])
      //浏览量
      val browserCountResp = DBHelper.find(s"SELECT v_source ,COUNT(1) count FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? GROUP BY v_source", List(start, end), classOf[JsonObject])
      //注册数
      val registerCountResp = DBHelper.find(s"SELECT source ,COUNT(1) count FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? GROUP BY source", List(start, end, UserOptEntity.FLAG_REGISTER), classOf[JsonObject])
      //申请数
      val applyCountResp = DBHelper.find(s"SELECT source ,COUNT(1) count FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? GROUP BY source", List(start, end, UserOptEntity.FLAG_APPLY), classOf[JsonObject])
      //自审通过数
      val selfExaminePassCountResp = DBHelper.find(s"SELECT source ,COUNT(1) count FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? GROUP BY source", List(start, end, UserOptEntity.FLAG_SELF_EXAMINE_PASS), classOf[JsonObject])
      //银审通过数
      val bankExaminePassCountResp = DBHelper.find(s"SELECT source ,COUNT(1) count FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? GROUP BY source", List(start, end, UserOptEntity.FLAG_BANK_EXAMINE_PASS), classOf[JsonObject])
      for {
        visitorCount <- visitorCountResp
        browserCount <- browserCountResp
        registerCount <- registerCountResp
        applyCount <- applyCountResp
        selfExaminePassCount <- selfExaminePassCountResp
        bankExaminePassCount <- bankExaminePassCountResp
      } yield {
        //key=source value=访客数,浏览量,注册数,申请数,自审通过数,银审通过数
        val result = collection.mutable.Map[String,ArrayBuffer[Long]]()
        visitorCount.body.foreach {
          record =>
            result += record.getString("v_source") -> ArrayBuffer[Long](record.getLong("count"),0,0,0,0,0)
        }
        browserCount.body.foreach {
          record =>
            if (result.contains(record.getString("v_source"))) {
              result(record.getString("v_source"))(1) += record.getLong("count")
            } else {
              result += record.getString("v_source") -> ArrayBuffer[Long](0,record.getLong("count"),0,0,0,0)
            }
        }
        registerCount.body.foreach {
          record =>
            if (result.contains(record.getString("source"))) {
              result(record.getString("source"))(2) += record.getLong("count")
            } else {
              result += record.getString("source") -> ArrayBuffer[Long](0,0,record.getLong("count"),0,0,0)
            }
        }
        applyCount.body.foreach {
          record =>
            if (result.contains(record.getString("source"))) {
              result(record.getString("source"))(3) += record.getLong("count")
            } else {
              result += record.getString("source") -> ArrayBuffer[Long](0,0,0,record.getLong("count"),0,0)
            }
        }
        selfExaminePassCount.body.foreach {
          record =>
            if (result.contains(record.getString("source"))) {
              result(record.getString("source"))(4) += record.getLong("count")
            } else {
              result += record.getString("source") -> ArrayBuffer[Long](0,0,0,0,record.getLong("count"),0)
            }
        }
        bankExaminePassCount.body.foreach {
          record =>
            if (result.contains(record.getString("source"))) {
              result(record.getString("source"))(5) += record.getLong("count")
            } else {
              result += record.getString("source") -> ArrayBuffer[Long](0,0,0,0,0,record.getLong("count"))
            }
        }
        p.success(Resp.success(result))
      }
    }
    p.future
  }

  def detailProcess(req: Map[String, String]): Future[Resp[Any]] = {
    val p = Promise[Resp[Any]]
    if (!req.contains("start") || !req.contains("end")) {
      p.success(Resp.badRequest("【start】【end】不能为空"))
    } else {
      val start = req("start").toLong
      val end = req("end").toLong
      val source = req.getOrElse("source", "")
      //访客数
      val visitorCountResp = if (source.nonEmpty)
        DBHelper.find(s"SELECT occur_date ,COUNT(DISTINCT visitor_id) count FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ?  AND v_source = ?  GROUP BY occur_date", List(start, end, source), classOf[JsonObject])
      else
        DBHelper.find(s"SELECT occur_date ,COUNT(DISTINCT visitor_id) count FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ?  GROUP BY occur_date", List(start, end), classOf[JsonObject])
      //浏览量
      val browserCountResp = if (source.nonEmpty)
        DBHelper.find(s"SELECT occur_date ,COUNT(1) count FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ?  AND v_source = ? GROUP BY occur_date", List(start, end, source), classOf[JsonObject])
      else
        DBHelper.find(s"SELECT occur_date ,COUNT(1) count FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? GROUP BY occur_date", List(start, end), classOf[JsonObject])
      //注册数
      val registerCountResp = if (source.nonEmpty)
        DBHelper.find(s"SELECT occur_date ,COUNT(1) count FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? AND source = ? GROUP BY occur_date", List(start, end, UserOptEntity.FLAG_REGISTER, source), classOf[JsonObject])
      else
        DBHelper.find(s"SELECT occur_date ,COUNT(1) count FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? GROUP BY occur_date", List(start, end, UserOptEntity.FLAG_REGISTER), classOf[JsonObject])
      //申请数
      val applyCountResp = if (source.nonEmpty)
        DBHelper.find(s"SELECT occur_date ,COUNT(1) count FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? AND source = ? GROUP BY occur_date", List(start, end, UserOptEntity.FLAG_APPLY, source), classOf[JsonObject])
      else
        DBHelper.find(s"SELECT occur_date ,COUNT(1) count FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? GROUP BY occur_date", List(start, end, UserOptEntity.FLAG_APPLY), classOf[JsonObject])
      //自审通过数
      val selfExaminePassCountResp = if (source.nonEmpty)
        DBHelper.find(s"SELECT occur_date ,COUNT(1) count FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? AND source = ? GROUP BY occur_date", List(start, end, UserOptEntity.FLAG_SELF_EXAMINE_PASS, source), classOf[JsonObject])
      else
        DBHelper.find(s"SELECT occur_date ,COUNT(1) count FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? GROUP BY occur_date", List(start, end, UserOptEntity.FLAG_SELF_EXAMINE_PASS), classOf[JsonObject])
      //银审通过数
      val bankExaminePassCountResp = if (source.nonEmpty)
        DBHelper.find(s"SELECT occur_date ,COUNT(1) count FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? AND source = ? GROUP BY occur_date", List(start, end, UserOptEntity.FLAG_BANK_EXAMINE_PASS, source), classOf[JsonObject])
      else
        DBHelper.find(s"SELECT occur_date ,COUNT(1) count FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? GROUP BY occur_date", List(start, end, UserOptEntity.FLAG_BANK_EXAMINE_PASS), classOf[JsonObject])
      for {
        visitorCount <- visitorCountResp
        browserCount <- browserCountResp
        registerCount <- registerCountResp
        applyCount <- applyCountResp
        selfExaminePassCount <- selfExaminePassCountResp
        bankExaminePassCount <- bankExaminePassCountResp
      } yield {
        //key=occur_date value=访客数,浏览量,注册数,申请数,自审通过数,银审通过数
        val result: Map[Long, ArrayBuffer[Long]] = getDateRange(start, end).map(i => i -> ArrayBuffer[Long](0,0,0,0,0,0)).toMap
        visitorCount.body.foreach {
          record =>
            result(record.getLong("occur_date"))(0) = record.getLong("count")
        }
        browserCount.body.foreach {
          record =>
            result(record.getLong("occur_date"))(1) = record.getLong("count")
        }
        registerCount.body.foreach {
          record =>
            result(record.getLong("occur_date"))(2) = record.getLong("count")
        }
        applyCount.body.foreach {
          record =>
            result(record.getLong("occur_date"))(3) = record.getLong("count")
        }
        selfExaminePassCount.body.foreach {
          record =>
            result(record.getLong("occur_date"))(4) = record.getLong("count")
        }
        bankExaminePassCount.body.foreach {
          record =>
            result(record.getLong("occur_date"))(5) = record.getLong("count")
        }
        p.success(Resp.success(result))
      }
    }
    p.future
  }

  override protected def process(req: Map[String, String], p: Promise[Resp[Any]]): Unit = {}
}
