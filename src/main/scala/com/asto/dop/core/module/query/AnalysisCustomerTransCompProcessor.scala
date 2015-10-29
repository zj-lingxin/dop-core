package com.asto.dop.core.module.query

import com.asto.dop.core.entity.{UserOptEntity, VisitEntity}
import com.asto.dop.core.helper.DBHelper
import com.ecfront.common.{JsonHelper, Resp}
import io.vertx.core.json.JsonObject

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

/**
 * 客户转化构成
 */
object AnalysisCustomerTransCompProcessor extends QueryProcessor {

  def platformSummaryProcess(req: Map[String, String]): Future[Resp[Any]] = {
    val p = Promise[Resp[Any]]
    if (!req.contains("start") || !req.contains("end")) {
      p.success(Resp.badRequest("【start】【end】不能为空"))
    } else {
      val start = req("start").toLong
      val end = req("end").toLong
      //访客数
      val visitorCountResp = DBHelper.find(s"SELECT c_platform AS platform,COUNT(DISTINCT visitor_id) AS count FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? GROUP BY  platform", List(start, end), classOf[JsonObject])
      //注册数
      val registerCountResp = DBHelper.find(s"SELECT platform AS platform,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? GROUP BY  platform", List(start, end, UserOptEntity.FLAG_REGISTER), classOf[JsonObject])
      //申请数
      val applyCountResp = DBHelper.find(s"SELECT platform AS platform,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? GROUP BY  platform", List(start, end, UserOptEntity.FLAG_APPLY), classOf[JsonObject])
      //自审通过数（在这段时间内申请并通过）
      val selfExaminePassCountResp = DBHelper.find(
        s"""
           |SELECT platform AS platform,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME}
           |     WHERE occur_date >= ?  AND occur_date <= ? AND action = ? AND user_id IN (
           |         SELECT user_id FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ?
           |     ) GROUP BY  platform
         """.stripMargin, List(start, end, UserOptEntity.FLAG_SELF_EXAMINE_PASS, start, end, UserOptEntity.FLAG_REGISTER), classOf[JsonObject])

      for {
        visitorCount <- visitorCountResp
        registerCount <- registerCountResp
        applyCount <- applyCountResp
        selfExaminePassCount <- selfExaminePassCountResp
      } yield {
        val platforms = List(VisitEntity.FLAG_PLATFORM_PC, VisitEntity.FLAG_PLATFORM_MOBILE)
        val visitorCountResult = platforms.map(p => p -> visitorCount.body.find(i => i.getString("platform") == p).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count")).toMap
        val registerCountResult = platforms.map(p => p -> registerCount.body.find(i => i.getString("platform") == p).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count")).toMap
        val applyCountResult = platforms.map(p => p -> applyCount.body.find(i => i.getString("platform") == p).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count")).toMap
        val selfExaminePassCountResult = platforms.map(p => p -> selfExaminePassCount.body.find(i => i.getString("platform") == p).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count")).toMap
        val registerRateResult = platforms.map(p => p -> (if (visitorCountResult(p) == 0) 0 else registerCountResult(p)*1.0 / visitorCountResult(p)))
        val applyRateResult = platforms.map(p => p -> (if (visitorCountResult(p) == 0) 0 else applyCountResult(p)*1.0 / visitorCountResult(p)))
        val visitorSelfExaminePassTransResult = platforms.map(p => p -> (if (visitorCountResult(p) == 0) 0 else selfExaminePassCountResult(p)*1.0 / visitorCountResult(p)))
        p.success(Resp.success(JsonHelper.toJson(
          s"""
             |{
             |    "visitorCount":${visitorCountResult.map(i => s"""{"platform":"${i._1}","count":${i._2}}""").mkString("[", ",", "]")},
             |    "registerCount":${registerCountResult.map(i => s"""{"platform":"${i._1}","count":${i._2}}""").mkString("[", ",", "]")},
             |    "applyCount":${applyCountResult.map(i => s"""{"platform":"${i._1}","count":${i._2}}""").mkString("[", ",", "]")},
             |    "registerRate":${registerRateResult.map(i => s"""{"platform":"${i._1}","rate":${i._2}}""").mkString("[", ",", "]")},
             |    "applyRate":${applyRateResult.map(i => s"""{"platform":"${i._1}","rate":${i._2}}""").mkString("[", ",", "]")},
             |    "visitorSelfExaminePassTransRate":${visitorSelfExaminePassTransResult.map(i => s"""{"platform":"${i._1}","rate":${i._2}}""").mkString("[", ",", "]")}
             |}
           """.stripMargin)))
      }
    }
    p.future
  }

  def platformTrendProcess(req: Map[String, String]): Future[Resp[Any]] = {
    val p = Promise[Resp[Any]]
    if (!req.contains("start") || !req.contains("end")) {
      p.success(Resp.badRequest("【start】【end】不能为空"))
    } else {
      val start = req("start").toLong
      val end = req("end").toLong
      val visitPlatformSql = if (req.contains("platform")) s" AND c_platform = '${req("platform")}'" else ""
      val userOptPlatformSql = if (req.contains("platform")) s" AND platform = '${req("platform")}'" else ""
      //访客数
      val visitorCountResp =
        DBHelper.find(s"SELECT occur_date, COUNT(DISTINCT visitor_id) AS count FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? $visitPlatformSql GROUP BY occur_date", List(start, end), classOf[JsonObject])
      //注册数
      val registerCountResp =
        DBHelper.find(s"SELECT occur_date, COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? $userOptPlatformSql GROUP BY occur_date", List(start, end, UserOptEntity.FLAG_REGISTER), classOf[JsonObject])
      //申请数
      val applyCountResp =
        DBHelper.find(s"SELECT occur_date,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? $userOptPlatformSql GROUP BY occur_date ", List(start, end, UserOptEntity.FLAG_APPLY), classOf[JsonObject])
      //自审通过数（在这段时间内申请并通过）
      val selfExaminePassCountResp = DBHelper.find(
        s"""
           |SELECT occur_date,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME}
           |     WHERE occur_date >= ?  AND occur_date <= ? AND action = ? AND user_id IN (
           |         SELECT user_id FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? $userOptPlatformSql
           |     )  GROUP BY occur_date
         """.stripMargin, List(start, end, UserOptEntity.FLAG_SELF_EXAMINE_PASS, start, end, UserOptEntity.FLAG_REGISTER), classOf[JsonObject])
      for {
        visitorCount <- visitorCountResp
        registerCount <- registerCountResp
        applyCount <- applyCountResp
        selfExaminePassCount <- selfExaminePassCountResp
      } yield {
        val dateGroup: Map[Long, Long] = getDateRange(start, end).map(i => i -> 0L).toMap
        val visitorCountResult = dateGroup.map(date => date._1 -> visitorCount.body.find(i => i.getLong("occur_date") == date._1).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count"))
        val registerCountResult = dateGroup.map(date => date._1 -> registerCount.body.find(i => i.getLong("occur_date") == date._1).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count"))
        val registerRateResult = dateGroup.map(date => date._1 -> (if (visitorCountResult(date._1) == 0) 0 else registerCountResult(date._1)*1.0 / visitorCountResult(date._1)))
        val applyCountResult = dateGroup.map(date => date._1 -> applyCount.body.find(i => i.getLong("occur_date") == date._1).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count"))
        val applyRateResult = dateGroup.map(date => date._1 -> (if (visitorCountResult(date._1) == 0) 0 else applyCountResult(date._1)*1.0 / visitorCountResult(date._1)))
        val selfExaminePassCountResult = dateGroup.map(date => date._1 -> selfExaminePassCount.body.find(i => i.getLong("occur_date") == date._1).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count"))
        val visitorSelfExaminePassTransResult = dateGroup.map(date => date._1 -> (if (visitorCountResult(date._1) == 0) 0 else selfExaminePassCountResult(date._1)*1.0 / visitorCountResult(date._1)))
        p.success(Resp.success(JsonHelper.toJson(
          s"""
             |{
             |    "registerCount":${registerCountResult.map(i => s"""{"occur_date":${i._1},"count":${i._2}}""").mkString("[", ",", "]")},
             |    "registerRate":${registerRateResult.map(i => s"""{"occur_date":${i._1},"rate":${i._2}}""").mkString("[", ",", "]")},
             |    "applyCount":${applyCountResult.map(i => s"""{"occur_date":${i._1},"count":${i._2}}""").mkString("[", ",", "]")},
             |    "applyRate":${applyRateResult.map(i => s"""{"occur_date":${i._1},"rate":${i._2}}""").mkString("[", ",", "]")},
             |    "visitorSelfExaminePassTrans":${visitorSelfExaminePassTransResult.map(i => s"""{"occur_date":${i._1},"rate":${i._2}}""").mkString("[", ",", "]")}
             |}
           """.stripMargin)))
      }
    }
    p.future
  }

  def visitorSummaryProcess(req: Map[String, String]): Future[Resp[Any]] = {
    val p = Promise[Resp[Any]]
    if (!req.contains("start") || !req.contains("end")) {
      p.success(Resp.badRequest("【start】【end】不能为空"))
    } else {
      val start = req("start").toLong
      val end = req("end").toLong
      //访客数
      val visitorCountResp = DBHelper.find(s"SELECT v_new_visitor AS new_visitor,COUNT(DISTINCT visitor_id) AS count FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? GROUP BY v_new_visitor", List(start, end), classOf[JsonObject])
      //新访客注册数
      val newVisitorRegisterCountResp = DBHelper.count(
        s"""
           |SELECT COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME}
           |     WHERE occur_date >= ?  AND occur_date <= ? AND action = ?  AND user_id IN (
           |        SELECT DISTINCT u_user_id FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND v_new_visitor = ?
           |     )
         """.stripMargin, List(start, end, UserOptEntity.FLAG_REGISTER, start, end, true))
      //老访客注册数
      val oldVisitorRegisterCountResp = DBHelper.count(
        s"""
           |SELECT COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME}
           |     WHERE occur_date >= ?  AND occur_date <= ? AND action = ?  AND user_id NOT IN (
           |        SELECT DISTINCT u_user_id FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND v_new_visitor = ?
           |     )
         """.stripMargin, List(start, end, UserOptEntity.FLAG_REGISTER, start, end, true))
      //新访客申请数
      val newVisitorApplyCountResp = DBHelper.count(
        s"""
           |SELECT COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME}
           |     WHERE occur_date >= ?  AND occur_date <= ? AND action = ?  AND user_id IN (
           |        SELECT DISTINCT u_user_id FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND v_new_visitor = ?
           |     )
         """.stripMargin, List(start, end, UserOptEntity.FLAG_APPLY, start, end, true))
      //老访客申请数
      val oldVisitorApplyCountResp = DBHelper.count(
        s"""
           |SELECT COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME}
           |     WHERE occur_date >= ?  AND occur_date <= ? AND action = ?  AND user_id NOT IN (
           |        SELECT DISTINCT u_user_id FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND v_new_visitor = ?
           |     )
         """.stripMargin, List(start, end, UserOptEntity.FLAG_APPLY, start, end, true))
      //新访客自审通过数（在这段时间内申请并通过）
      val newVisitorSelfExaminePassCountResp = DBHelper.count(
        s"""
           |SELECT COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME}
           |     WHERE occur_date >= ?  AND occur_date <= ? AND action = ? AND user_id IN (
           |         SELECT DISTINCT user_id FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ?
           |     ) AND user_id IN (
           |         SELECT DISTINCT u_user_id FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND v_new_visitor = ?
           |     )
         """.stripMargin, List(start, end, UserOptEntity.FLAG_SELF_EXAMINE_PASS, start, end, UserOptEntity.FLAG_REGISTER, start, end, true))
      //老访客自审通过数（在这段时间内申请并通过）
      val oldVisitorSelfExaminePassCountResp = DBHelper.count(
        s"""
           |SELECT COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME}
           |     WHERE occur_date >= ?  AND occur_date <= ? AND action = ? AND user_id IN (
           |         SELECT DISTINCT user_id FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ?
           |     ) AND user_id NOT IN (
           |         SELECT DISTINCT u_user_id FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND v_new_visitor = ?
           |     )
         """.stripMargin, List(start, end, UserOptEntity.FLAG_SELF_EXAMINE_PASS, start, end, UserOptEntity.FLAG_REGISTER, start, end, true))

      for {
        visitorCount <- visitorCountResp
        newVisitorRegisterCount <- newVisitorRegisterCountResp
        oldVisitorRegisterCount <- oldVisitorRegisterCountResp
        newVisitorApplyCount <- newVisitorApplyCountResp
        oldVisitorApplyCount <- oldVisitorApplyCountResp
        newVisitorSelfExaminePassCount <- newVisitorSelfExaminePassCountResp
        oldVisitorSelfExaminePassCount <- oldVisitorSelfExaminePassCountResp
      } yield {
        val newVisitorCount = visitorCount.body.find(i => i.getBoolean("new_visitor")).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count")
        val oldVisitorCount = visitorCount.body.find(i => !i.getBoolean("new_visitor")).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count")
        val registerCountResult = Map("new" -> newVisitorRegisterCount.body, "old" -> oldVisitorRegisterCount.body)
        val applyCountResult = Map("new" -> newVisitorApplyCount.body, "old" -> oldVisitorApplyCount.body)
        val registerRateResult = Map("new" -> (if (newVisitorCount == 0) 0 else newVisitorRegisterCount.body*1.0 / newVisitorCount), "old" -> (if (oldVisitorCount == 0) 0 else oldVisitorRegisterCount.body*1.0 / oldVisitorCount))
        val applyRateResult = Map("new" -> (if (newVisitorCount == 0) 0 else newVisitorApplyCount.body*1.0 / newVisitorCount), "old" -> (if (oldVisitorCount == 0) 0 else oldVisitorApplyCount.body*1.0 / oldVisitorCount))
        val visitorSelfExaminePassTransResult = Map("new" -> (if (newVisitorCount == 0) 0 else newVisitorSelfExaminePassCount.body*1.0 / newVisitorCount), "old" -> (if (oldVisitorCount == 0) 0 else oldVisitorSelfExaminePassCount.body*1.0 / oldVisitorCount))
        p.success(Resp.success(JsonHelper.toJson(
          s"""
             |{
             |    "visitorCount":[{"visit":"new","count":$newVisitorCount},{"visit":"old","count":$oldVisitorCount}],
             |    "registerCount":${registerCountResult.map(i => s"""{"visit":"${i._1}","count":${i._2}}""").mkString("[", ",", "]")},
             |    "applyCount":${applyCountResult.map(i => s"""{"visit":"${i._1}","count":${i._2}}""").mkString("[", ",", "]")},
             |    "registerRate":${registerRateResult.map(i => s"""{"visit":"${i._1}","rate":${i._2}}""").mkString("[", ",", "]")},
             |    "applyRate":${applyRateResult.map(i => s"""{"visit":"${i._1}","rate":${i._2}}""").mkString("[", ",", "]")},
             |    "visitorSelfExaminePassTransRate":${visitorSelfExaminePassTransResult.map(i => s"""{"visit":"${i._1}","rate":${i._2}}""").mkString("[", ",", "]")}
             |}
           """.stripMargin)))
      }
    }
    p.future
  }

  def visitorTrendProcess(req: Map[String, String]): Future[Resp[Any]] = {
    val p = Promise[Resp[Any]]
    if (!req.contains("start") || !req.contains("end")) {
      p.success(Resp.badRequest("【start】【end】不能为空"))
    } else {
      val start = req("start").toLong
      val end = req("end").toLong
      val visitPlatformSql = if (req.contains("platform")) s" AND c_platform = '${req("platform")}'" else ""
      val userOptPlatformSql = if (req.contains("platform")) s" AND platform = '${req("platform")}'" else ""
      val visitVisitorTypeSql = if (req.contains("newVisitor")) s" AND v_new_visitor = ${req("newVisitor").toBoolean}" else ""
      val userOptVisitorTypeSql = if (req.contains("newVisitor")) s" AND  user_id IN ( SELECT DISTINCT u_user_id FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= $start AND occur_date <= $end AND v_new_visitor = ${req("newVisitor").toBoolean} ) " else ""
      //访客数
      val visitorCountResp = DBHelper.find(s"SELECT occur_date,COUNT(DISTINCT visitor_id) AS count FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? $visitVisitorTypeSql $visitPlatformSql  GROUP BY occur_date", List(start, end), classOf[JsonObject])
      //注册数
      val registerCountResp = DBHelper.find(
        s"""
           |SELECT occur_date,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME}
           |     WHERE occur_date >= ?  AND occur_date <= ? AND action = ? $userOptPlatformSql $userOptVisitorTypeSql  GROUP BY occur_date
         """.stripMargin, List(start, end, UserOptEntity.FLAG_REGISTER), classOf[JsonObject])
      //申请数
      val applyCountResp = DBHelper.find(
        s"""
           |SELECT occur_date,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME}
           |     WHERE occur_date >= ?  AND occur_date <= ? AND action = ? $userOptPlatformSql $userOptVisitorTypeSql  GROUP BY occur_date
         """.stripMargin, List(start, end, UserOptEntity.FLAG_APPLY), classOf[JsonObject])
      //自审通过数（在这段时间内申请并通过）
      val selfExaminePassCountResp = DBHelper.find(
        s"""
           |SELECT occur_date,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME}
           |     WHERE occur_date >= ?  AND occur_date <= ? AND action = ? AND user_id IN (
           |         SELECT DISTINCT user_id FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ?
           |     )  $userOptPlatformSql  $userOptVisitorTypeSql GROUP BY occur_date
         """.stripMargin, List(start, end, UserOptEntity.FLAG_SELF_EXAMINE_PASS, start, end, UserOptEntity.FLAG_REGISTER), classOf[JsonObject])
      for {
        visitorCount <- visitorCountResp
        registerCount <- registerCountResp
        applyCount <- applyCountResp
        selfExaminePassCount <- selfExaminePassCountResp
      } yield {
        val dateGroup: Map[Long, Long] = getDateRange(start, end).map(i => i -> 0L).toMap
        val visitorCountResult = dateGroup.map(date => date._1 -> visitorCount.body.find(i => i.getLong("occur_date") == date._1).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count"))
        val registerCountResult = dateGroup.map(date => date._1 -> registerCount.body.find(i => i.getLong("occur_date") == date._1).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count"))
        val registerRateResult = dateGroup.map(date => date._1 -> (if (visitorCountResult(date._1) == 0) 0 else registerCountResult(date._1)*1.0 / visitorCountResult(date._1)))
        val applyCountResult = dateGroup.map(date => date._1 -> applyCount.body.find(i => i.getLong("occur_date") == date._1).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count"))
        val applyRateResult = dateGroup.map(date => date._1 -> (if (visitorCountResult(date._1) == 0) 0 else applyCountResult(date._1)*1.0 / visitorCountResult(date._1)))
        val selfExaminePassCountResult = dateGroup.map(date => date._1 -> selfExaminePassCount.body.find(i => i.getLong("occur_date") == date._1).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count"))
        val visitorSelfExaminePassTransResult = dateGroup.map(date => date._1 -> (if (visitorCountResult(date._1) == 0) 0 else selfExaminePassCountResult(date._1)*1.0 / visitorCountResult(date._1)))
        p.success(Resp.success(JsonHelper.toJson(
          s"""
             |{
             |    "registerCount":${registerCountResult.map(i => s"""{"occur_date":${i._1},"count":${i._2}}""").mkString("[", ",", "]")},
             |    "registerRate":${registerRateResult.map(i => s"""{"occur_date":${i._1},"rate":${i._2}}""").mkString("[", ",", "]")},
             |    "applyCount":${applyCountResult.map(i => s"""{"occur_date":${i._1},"count":${i._2}}""").mkString("[", ",", "]")},
             |    "applyRate":${applyRateResult.map(i => s"""{"occur_date":${i._1},"rate":${i._2}}""").mkString("[", ",", "]")},
             |    "visitorSelfExaminePassTrans":${visitorSelfExaminePassTransResult.map(i => s"""{"occur_date":${i._1},"rate":${i._2}}""").mkString("[", ",", "]")}
             |}
           """.stripMargin)))
      }
    }
    p.future
  }

  override protected def process(req: Map[String, String], p: Promise[Resp[Any]]): Unit = {}
}
