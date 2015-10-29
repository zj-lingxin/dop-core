package com.asto.dop.core.module.query

import com.asto.dop.core.entity.{UserOptEntity, VisitEntity}
import com.asto.dop.core.helper.DBHelper
import com.ecfront.common.{JsonHelper, Resp}
import io.vertx.core.json.JsonObject

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

/**
 * 客户转化概况
 */
object AnalysisCustomerTransProcessor extends QueryProcessor {

  def summaryProcess(req: Map[String, String]): Future[Resp[Any]] = {
    val p = Promise[Resp[Any]]
    if (!req.contains("start") || !req.contains("end")) {
      p.success(Resp.badRequest("【start】【end】不能为空"))
    } else {
      val start = req("start").toLong
      val end = req("end").toLong
      val qoqStart = start - (end - start) - 1
      val qoqEnd = end - (end - start) - 1
      val visitPlatformSql = if (req.contains("platform")) s" AND c_platform = '${req("platform")}'" else ""
      val userOptPlatformSql = if (req.contains("platform")) s" AND platform = '${req("platform")}'" else ""

      val (browserCountResp, visitorCountResp, registeredVisitorCountResp, registerCountResp, applyCountResp, applyAmountResp, selfExaminePassCountResp, selfExaminePassAmountResp, bankExaminePassCountResp, bankExaminePassAmountResp) = executeSummaryProcess(start, end, visitPlatformSql, userOptPlatformSql)
      val (qoqBrowserCountResp, qoqVisitorCountResp, qoqRegisteredVisitorCountResp, qoqRegisterCountResp, qoqApplyCountResp, qoqApplyAmountResp, qoqSelfExaminePassCountResp, qoqSelfExaminePassAmountResp, qoqBankExaminePassCountResp, qoqBankExaminePassAmountResp) = executeSummaryProcess(qoqStart, qoqEnd, visitPlatformSql, userOptPlatformSql)

      for {
        browserCount <- browserCountResp
        visitorCount <- visitorCountResp
        registeredVisitorCount <- registeredVisitorCountResp
        registerCount <- registerCountResp
        applyCount <- applyCountResp
        applyAmount <- applyAmountResp
        selfExaminePassCount <- selfExaminePassCountResp
        selfExaminePassAmount <- selfExaminePassAmountResp
        bankExaminePassCount <- bankExaminePassCountResp
        bankExaminePassAmount <- bankExaminePassAmountResp
        qoqBrowserCount <- qoqBrowserCountResp
        qoqVisitorCount <- qoqVisitorCountResp
        qoqRegisteredVisitorCount <- qoqRegisteredVisitorCountResp
        qoqRegisterCount <- qoqRegisterCountResp
        qoqApplyCount <- qoqApplyCountResp
        qoqApplyAmount <- qoqApplyAmountResp
        qoqSelfExaminePassCount <- qoqSelfExaminePassCountResp
        qoqSelfExaminePassAmount <- qoqSelfExaminePassAmountResp
        qoqBankExaminePassCount <- qoqBankExaminePassCountResp
        qoqBankExaminePassAmount <- qoqBankExaminePassAmountResp
      } yield {
        p.success(Resp.success(JsonHelper.toJson(
          s"""
             |{
             |    "browserCount":${browserCount.body},
             |    "visitorCount":${visitorCount.body},
             |    "registeredVisitorCount":${registeredVisitorCount.body},
             |    "registerCount":${registerCount.body},
             |    "applyCount":${applyCount.body},
             |    "applyAmount":${applyAmount.body.getLong("amount")},
             |    "selfExaminePassCount":${selfExaminePassCount.body},
             |    "selfExaminePassAmount":${selfExaminePassAmount.body.getLong("amount")},
             |    "bankExaminePassCount":${bankExaminePassCount.body},
             |    "bankExaminePassAmount":${bankExaminePassAmount.body.getLong("amount")},
             |    "qoqBrowserCount":${qoqBrowserCount.body},
             |    "qoqVisitorCount":${qoqVisitorCount.body},
             |    "qoqRegisteredVisitorCount":${qoqRegisteredVisitorCount.body},
             |    "qoqRegisterCount":${qoqRegisterCount.body},
             |    "qoqApplyCount":${qoqApplyCount.body},
             |    "qoqApplyAmount":${qoqApplyAmount.body.getLong("amount")},
             |    "qoqSelfExaminePassCount":${qoqSelfExaminePassCount.body},
             |    "qoqSelfExaminePassAmount":${qoqSelfExaminePassAmount.body.getLong("amount")},
             |    "qoqBankExaminePassCount":${qoqBankExaminePassCount.body},
             |    "qoqBankExaminePassAmount":${qoqBankExaminePassAmount.body.getLong("amount")}
             |}
           """.stripMargin)))
      }
    }
    p.future
  }

  private def executeSummaryProcess(start: Long, end: Long, visitPlatformSql: String, userOptPlatformSql: String) = {
    //浏览量
    val browserCountResp = DBHelper.count(s"SELECT COUNT(1) AS count FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? $visitPlatformSql", List(start, end))
    //访客数
    val visitorCountResp = DBHelper.count(s"SELECT COUNT(DISTINCT visitor_id) AS count FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? $visitPlatformSql", List(start, end))
    //已注册过的访客数
    val registeredVisitorCountResp = DBHelper.count(s"SELECT COUNT(DISTINCT visitor_id) AS count FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND u_user_id !='' $visitPlatformSql", List(start, end))
    //注册数
    val registerCountResp = DBHelper.count(s"SELECT COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? $userOptPlatformSql", List(start, end, UserOptEntity.FLAG_REGISTER))
    //申请数
    val applyCountResp = DBHelper.count(s"SELECT COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? $userOptPlatformSql", List(start, end, UserOptEntity.FLAG_APPLY))
    //参考授信
    val applyAmountResp = DBHelper.get(s"SELECT SUM(amount) AS amount FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? $userOptPlatformSql", List(start, end, UserOptEntity.FLAG_APPLY), classOf[JsonObject])
    //自审通过数（在这段时间内申请并通过）
    val selfExaminePassCountResp = DBHelper.count(
      s"""
         |SELECT COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME}
         |     WHERE occur_date >= ?  AND occur_date <= ? AND action = ? AND user_id IN (
         |         SELECT DISTINCT user_id FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? $userOptPlatformSql
         |     )
         """.stripMargin, List(start, end, UserOptEntity.FLAG_SELF_EXAMINE_PASS, start, end, UserOptEntity.FLAG_REGISTER))
    //平台授信
    val
    selfExaminePassAmountResp = DBHelper.get(
      s"""
         |SELECT SUM(amount) AS amount FROM ${UserOptEntity.db.TABLE_NAME}
         |     WHERE occur_date >= ?  AND occur_date <= ? AND action = ? AND user_id IN (
         |         SELECT DISTINCT user_id FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? $userOptPlatformSql
         |     )
         """.stripMargin,
      List(start, end, UserOptEntity.FLAG_SELF_EXAMINE_PASS, start, end, UserOptEntity.FLAG_REGISTER), classOf[JsonObject])
    //银审通过数（在这段时间内申请并通过）
    val
    bankExaminePassCountResp = DBHelper.count(
      s"""
         |SELECT COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME}
         |     WHERE occur_date >= ?  AND occur_date <= ? AND action = ? AND user_id IN (
         |         SELECT DISTINCT user_id FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? $userOptPlatformSql
         |     )
         """.
        stripMargin, List(start, end, UserOptEntity.FLAG_BANK_EXAMINE_PASS, start, end, UserOptEntity.FLAG_REGISTER))
    //银行授信
    val bankExaminePassAmountResp = DBHelper.get(
      s"""
         |SELECT SUM(amount) AS amount FROM ${UserOptEntity.db.TABLE_NAME}
         |     WHERE occur_date >= ?  AND occur_date <= ? AND action = ? AND user_id IN (
         |         SELECT DISTINCT user_id FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? $userOptPlatformSql
         |     )
         """.stripMargin,
      List(start, end, UserOptEntity.FLAG_BANK_EXAMINE_PASS, start, end, UserOptEntity.FLAG_REGISTER), classOf[JsonObject])
    (browserCountResp, visitorCountResp, registeredVisitorCountResp, registerCountResp, applyCountResp, applyAmountResp, selfExaminePassCountResp, selfExaminePassAmountResp, bankExaminePassCountResp, bankExaminePassAmountResp)
  }

  def trendProcess(req: Map[String, String]): Future[Resp[Any]] = {
    val p = Promise[Resp[Any]]
    if (!req.contains("start") || !req.contains("end")) {
      p.success(Resp.badRequest("【start】【end】不能为空"))
    } else {
      val start = req("start").toLong
      val end = req("end").toLong
      val visitPlatformSql = if (req.contains("platform")) s" AND c_platform = '${req("platform")}'" else ""
      val userOptPlatformSql = if (req.contains("platform")) s" AND platform = '${req("platform")}'" else ""
      //浏览量
      val browserCountResp =
        DBHelper.find(s"SELECT occur_date, COUNT(1) AS count FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? $visitPlatformSql GROUP BY occur_date", List(start, end), classOf[JsonObject])
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
           |         SELECT DISTINCT user_id FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? $userOptPlatformSql
           |     )  GROUP BY occur_date
         """.stripMargin, List(start, end, UserOptEntity.FLAG_SELF_EXAMINE_PASS, start, end, UserOptEntity.FLAG_REGISTER), classOf[JsonObject])
      //银审通过数（在这段时间内申请并通过）
      val bankExaminePassCountResp = DBHelper.find(
        s"""
           |SELECT occur_date,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME}
           |     WHERE occur_date >= ?  AND occur_date <= ? AND action = ? AND user_id IN (
           |         SELECT DISTINCT user_id FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? $userOptPlatformSql
           |     )  GROUP BY occur_date
         """.stripMargin, List(start, end, UserOptEntity.FLAG_BANK_EXAMINE_PASS, start, end, UserOptEntity.FLAG_REGISTER), classOf[JsonObject])
      //参考授信
      val applyAmountResp =
          DBHelper.find(s"SELECT occur_date, SUM(amount) AS amount FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? $userOptPlatformSql GROUP BY occur_date ", List(start, end, UserOptEntity.FLAG_APPLY), classOf[JsonObject])
      //银行授信
      val bankExaminePassAmountResp =
          DBHelper.find(
            s"""
               |SELECT occur_date,SUM(amount) AS amount FROM ${UserOptEntity.db.TABLE_NAME}
               |     WHERE occur_date >= ?  AND occur_date <= ? AND action = ? AND user_id IN (
               |         SELECT DISTINCT user_id FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ? AND action = ? $userOptPlatformSql
               |     )  GROUP BY occur_date
         """.stripMargin,
            List(start, end, UserOptEntity.FLAG_BANK_EXAMINE_PASS, start, end, UserOptEntity.FLAG_REGISTER), classOf[JsonObject])

      for {
        browserCount <- browserCountResp
        visitorCount <- visitorCountResp
        registerCount <- registerCountResp
        applyCount <- applyCountResp
        selfExaminePassCount <- selfExaminePassCountResp
        bankExaminePassCount <- bankExaminePassCountResp
        applyAmount <- applyAmountResp
        bankExaminePassAmount <- bankExaminePassAmountResp
      } yield {
        val dateGroup: Map[Long, Long] =getDateRange(start,end).map( i => i -> 0L).toMap
        val browserCountResult = dateGroup.map(date => date._1 -> browserCount.body.find(i => i.getLong("occur_date") == date._1).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count"))
        val visitorCountResult = dateGroup.map(date => date._1 -> visitorCount.body.find(i => i.getLong("occur_date") == date._1).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count"))
        val registerCountResult = dateGroup.map(date => date._1 -> registerCount.body.find(i => i.getLong("occur_date") == date._1).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count"))
        val applyCountResult = dateGroup.map(date => date._1 -> applyCount.body.find(i => i.getLong("occur_date") == date._1).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count"))
        val selfExaminePassCountResult = dateGroup.map(date => date._1 -> selfExaminePassCount.body.find(i => i.getLong("occur_date") == date._1).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count"))
        val bankExaminePassCountResult = dateGroup.map(date => date._1 -> bankExaminePassCount.body.find(i => i.getLong("occur_date") == date._1).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count"))
        val applyAmountResult = dateGroup.map(date => date._1 -> applyAmount.body.find(i => i.getLong("occur_date") == date._1).getOrElse(new JsonObject( s"""{"amount":0}""")).getLong("amount"))
        val bankExaminePassAmountResult = dateGroup.map(date => date._1 -> bankExaminePassAmount.body.find(i => i.getLong("occur_date") == date._1).getOrElse(new JsonObject( s"""{"amount":0}""")).getLong("amount"))
        val visitorTransResult = dateGroup.map(date => date._1 -> (if (browserCountResult(date._1) == 0) 0 else visitorCountResult(date._1)*1.0 / browserCountResult(date._1)))
        val visitorRegisterTransResult = dateGroup.map(date => date._1 -> (if (visitorCountResult(date._1) == 0) 0 else registerCountResult(date._1)*1.0 / visitorCountResult(date._1)))
        val applyTransResult = dateGroup.map(date => date._1 -> (if (registerCountResult(date._1) == 0) 0 else applyCountResult(date._1)*1.0 / registerCountResult(date._1)))
        val selfExaminePassTransResult = dateGroup.map(date => date._1 -> (if (applyCountResult(date._1) == 0) 0 else selfExaminePassCountResult(date._1)*1.0 / applyCountResult(date._1)))
        val bankExaminePassTransResult = dateGroup.map(date => date._1 -> (if (selfExaminePassCountResult(date._1) == 0) 0 else bankExaminePassCountResult(date._1)*1.0 / selfExaminePassCountResult(date._1)))
        val visitorSelfExaminePassTransResult = dateGroup.map(date => date._1 -> (if (visitorCountResult(date._1) == 0) 0 else selfExaminePassCountResult(date._1)*1.0 / visitorCountResult(date._1)))
        p.success(Resp.success(JsonHelper.toJson(
          s"""
             |{
             |    "browserCount":${browserCountResult.map(i => s"""{"occur_date":${i._1},"count":${i._2}}""").mkString("[", ",", "]")},
             |    "visitorCount":${visitorCountResult.map(i => s"""{"occur_date":${i._1},"count":${i._2}}""").mkString("[", ",", "]")},
             |    "registerCount":${registerCountResult.map(i => s"""{"occur_date":${i._1},"count":${i._2}}""").mkString("[", ",", "]")},
             |    "applyCount":${applyCountResult.map(i => s"""{"occur_date":${i._1},"count":${i._2}}""").mkString("[", ",", "]")},
             |    "selfExaminePassCount":${selfExaminePassCountResult.map(i => s"""{"occur_date":${i._1},"count":${i._2}}""").mkString("[", ",", "]")},
             |    "bankExaminePassCount":${bankExaminePassCountResult.map(i => s"""{"occur_date":${i._1},"count":${i._2}}""").mkString("[", ",", "]")},
             |    "applyAmount":${applyAmountResult.map(i => s"""{"occur_date":${i._1},"amount":${i._2}}""").mkString("[", ",", "]")},
             |    "bankExaminePassAmount":${bankExaminePassAmountResult.map(i => s"""{"occur_date":${i._1},"amount":${i._2}}""").mkString("[", ",", "]")},
             |    "visitorTrans":${visitorTransResult.map(i => s"""{"occur_date":${i._1},"rate":${i._2}}""").mkString("[", ",", "]")},
             |    "visitorRegisterTrans":${visitorRegisterTransResult.map(i => s"""{"occur_date":${i._1},"rate":${i._2}}""").mkString("[", ",", "]")},
             |    "applyTrans":${applyTransResult.map(i => s"""{"occur_date":${i._1},"rate":${i._2}}""").mkString("[", ",", "]")},
             |    "selfExaminePassTrans":${selfExaminePassTransResult.map(i => s"""{"occur_date":${i._1},"rate":${i._2}}""").mkString("[", ",", "]")},
             |    "bankExaminePassTrans":${bankExaminePassTransResult.map(i => s"""{"occur_date":${i._1},"rate":${i._2}}""").mkString("[", ",", "]")},
             |    "visitorSelfExaminePassTrans":${visitorSelfExaminePassTransResult.map(i => s"""{"occur_date":${i._1},"rate":${i._2}}""").mkString("[", ",", "]")}
             |}
           """.stripMargin)))
      }
    }
    p.future
  }

  override protected def process(req: Map[String, String], p: Promise[Resp[Any]]): Unit = {}
}
