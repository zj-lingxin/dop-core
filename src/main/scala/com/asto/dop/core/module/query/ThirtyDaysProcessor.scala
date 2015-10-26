package com.asto.dop.core.module.query

import java.util.Date

import com.asto.dop.core.helper.DBHelper
import com.asto.dop.core.entity.{UserOptEntity, VisitEntity}
import com.ecfront.common.{JsonHelper, Resp}
import io.vertx.core.json.JsonObject

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Promise

/**
 * 最近30天运营概况（主页）
 */
object ThirtyDaysProcessor extends QueryProcessor {

  override protected def process(req: Map[String, String], p: Promise[Resp[Any]]): Unit = {
    val today = df.format(new Date()).substring(0, 8).toLong
    val thirtyDayRange = (today - 31, today)
    val lastMonthDayRange = (today - 62, today - 32)
    //30天+上个月浏览量
    val browserCountResp = DBHelper.find(s"SELECT occur_date,COUNT(1) AS count FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ? AND occur_date <= ? GROUP BY occur_date", List(lastMonthDayRange._1, thirtyDayRange._2), classOf[JsonObject])
    //30天+上个月访客数
    val visitorCountResp = DBHelper.find(s"SELECT occur_date,COUNT(DISTINCT visitor_id) AS count FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ? AND occur_date <= ? GROUP BY occur_date", List(lastMonthDayRange._1, thirtyDayRange._2), classOf[JsonObject])
    //30天+上个月注册数
    val registerCountResp = DBHelper.find(s"SELECT occur_date,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ? AND occur_date <= ? AND action = ? GROUP BY occur_date", List(lastMonthDayRange._1, thirtyDayRange._2, UserOptEntity.FLAG_REGISTER), classOf[JsonObject])
    //30天+上个月申请数、金额
    val applyCountResp = DBHelper.find(s"SELECT occur_date,COUNT(1) AS count,SUM(amount) AS amount FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ? AND occur_date <= ? AND action = ? GROUP BY occur_date", List(lastMonthDayRange._1, thirtyDayRange._2, UserOptEntity.FLAG_APPLY), classOf[JsonObject])
    //30天+上个月自审通过数、金额
    val selfExaminePassCountResp = DBHelper.find(s"SELECT occur_date,COUNT(1) AS count,SUM(amount) AS amount FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ? AND occur_date <= ? AND action = ? GROUP BY occur_date", List(lastMonthDayRange._1, thirtyDayRange._2, UserOptEntity.FLAG_SELF_EXAMINE_PASS), classOf[JsonObject])
    //30天+上个月银审通过数、金额
    val bankExaminePassCountResp = DBHelper.find(s"SELECT occur_date,COUNT(1) AS count,SUM(amount) AS amount FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ? AND occur_date <= ? AND action = ? GROUP BY occur_date", List(lastMonthDayRange._1, thirtyDayRange._2, UserOptEntity.FLAG_BANK_EXAMINE_PASS), classOf[JsonObject])
    for {
      browserCount <- browserCountResp
      visitorCount <- visitorCountResp
      registerCount <- registerCountResp
      applyCount <- applyCountResp
      selfExaminePassCount <- selfExaminePassCountResp
      bankExaminePassCount <- bankExaminePassCountResp
    } yield {
      val thirtyDaysBrowserCount = browserCount.body.filter(record => record.getInteger("occur_date") >= thirtyDayRange._1)
      val lastMonthBrowserCount = browserCount.body.filter(record => record.getInteger("occur_date") < thirtyDayRange._1)
      val browserCount_thirtyDays=if(thirtyDaysBrowserCount.nonEmpty) thirtyDaysBrowserCount.map(_.getLong("count")).reduce(_+_) else 0
      val browserCount_lastMonth=if(lastMonthBrowserCount.nonEmpty) lastMonthBrowserCount.map(_.getLong("count")).reduce(_+_) else 0
      val browserCount_daily=if(thirtyDaysBrowserCount.nonEmpty) thirtyDaysBrowserCount.map(record => s"[${record.getInteger("occur_date")},${record.getLong("count")}]").mkString("[",",","]") else "[]"

      val thirtyDaysVisitorCount = visitorCount.body.filter(record => record.getInteger("occur_date") >= thirtyDayRange._1)
      val lastMonthVisitorCount = visitorCount.body.filter(record => record.getInteger("occur_date") < thirtyDayRange._1)
      val visitorCount_thirtyDays=if(thirtyDaysVisitorCount.nonEmpty) thirtyDaysVisitorCount.map(_.getLong("count")).reduce(_+_) else 0
      val visitorCount_lastMonth=if(lastMonthVisitorCount.nonEmpty) lastMonthVisitorCount.map(_.getLong("count")).reduce(_+_) else 0
      val visitorCount_daily=if(thirtyDaysVisitorCount.nonEmpty) thirtyDaysVisitorCount.map(record => s"[${record.getInteger("occur_date")},${record.getLong("count")}]").mkString("[",",","]") else "[]"

      val thirtyDaysRegisterCount = registerCount.body.filter(record => record.getInteger("occur_date") >= thirtyDayRange._1)
      val lastMonthRegisterCount = registerCount.body.filter(record => record.getInteger("occur_date") < thirtyDayRange._1)
      val registerCount_thirtyDays=if(thirtyDaysRegisterCount.nonEmpty) thirtyDaysRegisterCount.map(_.getLong("count")).reduce(_+_) else 0
      val registerCount_lastMonth=if(lastMonthRegisterCount.nonEmpty) lastMonthRegisterCount.map(_.getLong("count")).reduce(_+_) else 0
      val registerCount_daily=if(thirtyDaysRegisterCount.nonEmpty) thirtyDaysRegisterCount.map(record => s"[${record.getInteger("occur_date")},${record.getLong("count")}]").mkString("[",",","]") else "[]"

      val thirtyDaysApplyCount = applyCount.body.filter(record => record.getInteger("occur_date") >= thirtyDayRange._1)
      val lastMonthApplyCount = applyCount.body.filter(record => record.getInteger("occur_date") < thirtyDayRange._1)
      val applyCount_thirtyDays=if(thirtyDaysApplyCount.nonEmpty) thirtyDaysApplyCount.map(_.getLong("count")).reduce(_+_) else 0
      val applyCount_lastMonth=if(lastMonthApplyCount.nonEmpty) lastMonthApplyCount.map(_.getLong("count")).reduce(_+_) else 0
      val applyCount_daily=if(thirtyDaysApplyCount.nonEmpty) thirtyDaysApplyCount.map(record => s"[${record.getInteger("occur_date")},${record.getLong("count")}]").mkString("[",",","]") else "[]"
      val applyAmount_thirtyDays=if(thirtyDaysApplyCount.nonEmpty) thirtyDaysApplyCount.map(_.getLong("amount")).reduce(_+_) else 0
      val applyAmount_lastMonth=if(lastMonthApplyCount.nonEmpty) lastMonthApplyCount.map(_.getLong("amount")).reduce(_+_) else 0
      val applyAmount_daily=if(thirtyDaysApplyCount.nonEmpty) thirtyDaysApplyCount.map(record => s"[${record.getInteger("occur_date")},${record.getLong("amount")}]").mkString("[",",","]") else "[]"

      val thirtyDaysSelfExaminePassCount = selfExaminePassCount.body.filter(record => record.getInteger("occur_date") >= thirtyDayRange._1)
      val lastMonthSelfExaminePassCount = selfExaminePassCount.body.filter(record => record.getInteger("occur_date") < thirtyDayRange._1)
      val selfExaminePassCount_thirtyDays=if(thirtyDaysSelfExaminePassCount.nonEmpty) thirtyDaysSelfExaminePassCount.map(_.getLong("count")).reduce(_+_) else 0
      val selfExaminePassCount_lastMonth=if(lastMonthSelfExaminePassCount.nonEmpty) lastMonthSelfExaminePassCount.map(_.getLong("count")).reduce(_+_) else 0
      val selfExaminePassCount_daily=if(thirtyDaysSelfExaminePassCount.nonEmpty) thirtyDaysSelfExaminePassCount.map(record => s"[${record.getInteger("occur_date")},${record.getLong("count")}]").mkString("[",",","]") else "[]"
      val selfExaminePassAmount_thirtyDays=if(thirtyDaysSelfExaminePassCount.nonEmpty) thirtyDaysSelfExaminePassCount.map(_.getLong("amount")).reduce(_+_) else 0
      val selfExaminePassAmount_lastMonth=if(lastMonthSelfExaminePassCount.nonEmpty) lastMonthSelfExaminePassCount.map(_.getLong("amount")).reduce(_+_) else 0
      val selfExaminePassAmount_daily=if(thirtyDaysSelfExaminePassCount.nonEmpty) thirtyDaysSelfExaminePassCount.map(record => s"[${record.getInteger("occur_date")},${record.getLong("amount")}]").mkString("[",",","]") else "[]"

      val thirtyDaysBankExaminePassCount = bankExaminePassCount.body.filter(record => record.getInteger("occur_date") >= thirtyDayRange._1)
      val lastMonthBankExaminePassCount = bankExaminePassCount.body.filter(record => record.getInteger("occur_date") < thirtyDayRange._1)
      val bankExaminePassCount_thirtyDays=if(thirtyDaysBankExaminePassCount.nonEmpty) thirtyDaysBankExaminePassCount.map(_.getLong("count")).reduce(_+_) else 0
      val bankExaminePassCount_lastMonth=if(lastMonthBankExaminePassCount.nonEmpty) lastMonthBankExaminePassCount.map(_.getLong("count")).reduce(_+_) else 0
      val bankExaminePassCount_daily=if(thirtyDaysBankExaminePassCount.nonEmpty) thirtyDaysBankExaminePassCount.map(record => s"[${record.getInteger("occur_date")},${record.getLong("count")}]").mkString("[",",","]") else "[]"
      val bankExaminePassAmount_thirtyDays=if(thirtyDaysBankExaminePassCount.nonEmpty) thirtyDaysBankExaminePassCount.map(_.getLong("amount")).reduce(_+_) else 0
      val bankExaminePassAmount_lastMonth=if(lastMonthBankExaminePassCount.nonEmpty) lastMonthBankExaminePassCount.map(_.getLong("amount")).reduce(_+_) else 0
      val bankExaminePassAmount_daily=if(thirtyDaysBankExaminePassCount.nonEmpty) thirtyDaysBankExaminePassCount.map(record => s"[${record.getInteger("occur_date")},${record.getLong("amount")}]").mkString("[",",","]") else "[]"

      p.success(Resp.success(JsonHelper.toJson(
       s"""
           |{
           |    "browserCount":{
           |         "thirtyDays":$browserCount_thirtyDays,
           |         "lastMonth":$browserCount_lastMonth,
           |         "daily":$browserCount_daily
           |    },
           |    "visitorCount":{
           |         "thirtyDays":$visitorCount_thirtyDays,
           |         "lastMonth":$visitorCount_lastMonth,
           |         "daily":$visitorCount_daily
           |    },
           |    "registerCount":{
           |         "thirtyDays":$registerCount_thirtyDays,
           |         "lastMonth":$registerCount_lastMonth,
           |         "daily":$registerCount_daily
           |    },
           |    "applyCount":{
           |         "thirtyDays":$applyCount_thirtyDays,
           |         "lastMonth":$applyCount_lastMonth,
           |         "daily":$applyCount_daily
           |    },
           |    "applyAmount":{
           |         "thirtyDays":$applyAmount_thirtyDays,
           |         "lastMonth":$applyAmount_lastMonth,
           |         "daily":$applyAmount_daily
           |    },
           |    "selfExaminePassCount":{
           |         "thirtyDays":$selfExaminePassCount_thirtyDays,
           |         "lastMonth":$selfExaminePassCount_lastMonth,
           |         "daily":$selfExaminePassCount_daily
           |    },
           |    "selfExaminePassAmount":{
           |         "thirtyDays":$selfExaminePassAmount_thirtyDays,
           |         "lastMonth":$selfExaminePassAmount_lastMonth,
           |         "daily":$selfExaminePassAmount_daily
           |    },
           |    "bankExaminePassCount":{
           |         "thirtyDays":$bankExaminePassCount_thirtyDays,
           |         "lastMonth":$bankExaminePassCount_lastMonth,
           |         "daily":$bankExaminePassCount_daily
           |    },
           |    "bankExaminePassAmount":{
           |         "thirtyDays":$bankExaminePassAmount_thirtyDays,
           |         "lastMonth":$bankExaminePassAmount_lastMonth,
           |         "daily":$bankExaminePassAmount_daily
           |    }
           |}
        """.stripMargin)))
    }
  }

}
