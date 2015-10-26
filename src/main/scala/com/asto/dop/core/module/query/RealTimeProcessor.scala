package com.asto.dop.core.module.query

import java.util.Date

import com.asto.dop.core.helper.DBHelper
import com.asto.dop.core.entity.{UserOptEntity, VisitEntity}
import com.ecfront.common.{JsonHelper, Resp}
import com.fasterxml.jackson.databind.JsonNode
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Promise

/**
 * 实时指标（主页）
 */
object RealTimeProcessor extends QueryProcessor {

  override protected def process(req: Map[String, String], p: Promise[Resp[Any]]): Unit = {
    val today = df.format(new Date()).substring(0, 8).toLong
    val yesterday = today - 1
    //今天访客数
    val todayVisitorCountResp =DBHelper.count(s"SELECT COUNT(DISTINCT visitor_id) FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date =? ", List(today))
    //今天移动访客数
    val todayMobileVisitorCountResp = DBHelper.count(s"SELECT COUNT(DISTINCT visitor_id) FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date =?  AND c_platform = ?", List(today, VisitEntity.FLAG_PLATFORM_MOBILE))
    //昨天访客数
    val yesterdayVisitorCountResp = DBHelper.count(s"SELECT COUNT(DISTINCT visitor_id) FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date =? ", List(yesterday))
    //今天浏览数
    val todayBrowserCountResp = VisitEntity.db.count("occur_date =? ", List(today))
    //今天移动浏览数
    val todayMobileBrowserCountResp = VisitEntity.db.count("occur_date =? AND c_platform = ? ", List(today, VisitEntity.FLAG_PLATFORM_MOBILE))
    //昨天浏览数
    val yesterdayBrowserCountResp = VisitEntity.db.count("occur_date =? ", List(yesterday))
    //今天注册数
    val todayRegisterCountResp = UserOptEntity.db.count("occur_date =? AND action = ? ", List(today, UserOptEntity.FLAG_REGISTER))
    //今天移动注册数
    val todayMobileRegisterCountResp = UserOptEntity.db.count("occur_date =? AND action = ? AND platform = ? ", List(today, UserOptEntity.FLAG_REGISTER, VisitEntity.FLAG_PLATFORM_MOBILE))
    //昨天注册数
    val yesterdayRegisterCountResp = UserOptEntity.db.count("occur_date =? AND action = ? ", List(yesterday, UserOptEntity.FLAG_REGISTER))
    //今天绑店数
    val todayBindCountResp = UserOptEntity.db.count("occur_date =? AND action = ? ", List(today, UserOptEntity.FLAG_BIND))
    //今天移动绑店数
    val todayMobileBindCountResp = UserOptEntity.db.count("occur_date =? AND action = ? AND platform = ? ", List(today, UserOptEntity.FLAG_BIND, VisitEntity.FLAG_PLATFORM_MOBILE))
    //昨天绑店数
    val yesterdayBindCountResp = UserOptEntity.db.count("occur_date =? AND action = ?  ", List(yesterday, UserOptEntity.FLAG_BIND))
    //今天申请数
    val todayApplyCountResp = UserOptEntity.db.count("occur_date =? AND action = ? ", List(today, UserOptEntity.FLAG_APPLY))
    //今天移动申请数
    val todayMobileApplyCountResp = UserOptEntity.db.count("occur_date =? AND action = ? AND platform = ? ", List(today, UserOptEntity.FLAG_APPLY, VisitEntity.FLAG_PLATFORM_MOBILE))
    //昨天申请数
    val yesterdayApplyCountResp = UserOptEntity.db.count("occur_date =? AND action = ?  ", List(yesterday, UserOptEntity.FLAG_APPLY))
    for {
      todayVisitorCount <- todayVisitorCountResp
      todayMobileVisitorCount <- todayMobileVisitorCountResp
      yesterdayVisitorCount <- yesterdayVisitorCountResp
      todayBrowserCount <- todayBrowserCountResp
      todayMobileBrowserCount <- todayMobileBrowserCountResp
      yesterdayBrowserCount <- yesterdayBrowserCountResp
      todayRegisterCount <- todayRegisterCountResp
      todayMobileRegisterCount <- todayMobileRegisterCountResp
      yesterdayRegisterCount <- yesterdayRegisterCountResp
      todayBindCount <- todayBindCountResp
      todayMobileBindCount <- todayMobileBindCountResp
      yesterdayBindCount <- yesterdayBindCountResp
      todayApplyCount <- todayApplyCountResp
      todayMobileApplyCount <- todayMobileApplyCountResp
      yesterdayApplyCount <- yesterdayApplyCountResp
    } yield {
      p.success(Resp.success(JsonHelper.toJson(s"""
                                                  |{
                                                  |    "todayVisitorCount":${todayVisitorCount.body},
                                                  |    "todayMobileVisitorCount":${todayMobileVisitorCount.body},
                                                  |    "yesterdayVisitorCount":${yesterdayVisitorCount.body},
                                                  |    "todayBrowserCount":${todayBrowserCount.body},
                                                  |    "todayMobileBrowserCount":${todayMobileBrowserCount.body},
                                                  |    "yesterdayBrowserCount":${yesterdayBrowserCount.body},
                                                  |    "todayRegisterCount":${todayRegisterCount.body},
                                                  |    "todayMobileRegisterCount":${todayMobileRegisterCount.body},
                                                  |    "yesterdayRegisterCount":${yesterdayRegisterCount.body},
                                                  |    "todayBindCount":${todayBindCount.body},
                                                  |    "todayMobileBindCount":${todayMobileBindCount.body},
                                                  |    "yesterdayBindCount":${yesterdayBindCount.body},
                                                  |    "todayApplyCount":${todayApplyCount.body},
                                                  |    "todayMobileApplyCount":${todayMobileApplyCount.body},
                                                  |    "yesterdayApplyCount":${yesterdayApplyCount.body}
                                                  |}
        """.stripMargin)))
    }
  }

}
