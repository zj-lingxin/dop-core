package com.asto.dop.core.module.query

import com.asto.dop.core.entity.{UserOptEntity, VisitEntity}
import com.asto.dop.core.helper.DBHelper
import com.ecfront.common.{JsonHelper, Resp}
import io.vertx.core.json.JsonObject

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Promise

/**
 * 流量分析（以下所有的数据都为最近7天的数据）
 * 访客-自审通过率：访客，某段时间内有访问记录，不关心是不是新老访客，自审通过率，某段时间内申请并通过的记录
 */
object TrafficAnalysisProcessor extends QueryProcessor {

  override protected def process(req: Map[String, String], p: Promise[Resp[Any]]): Unit = {
    val yesterday = dateOffset(-1)
    val last7Day = dateOffset(-7)

    val pcVisitorCountTopResp = DBHelper.find(s"SELECT v_source,COUNT(DISTINCT visitor_id) AS count FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ? AND occur_date <= ?  AND c_platform = ? GROUP BY  v_source ORDER BY count DESC LIMIT 5 ", List(last7Day,yesterday, VisitEntity.FLAG_PLATFORM_PC), classOf[JsonObject])

    val pcSelfCountResp = DBHelper.find(
      s"""
         |SELECT source,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME}
         |     WHERE occur_date >= ? AND occur_date <= ? AND action = ? AND id IN (
         |         SELECT id FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ? AND occur_date <= ? AND  action = ? AND platform = ?
         |     )  GROUP BY  source
       """.stripMargin, List(last7Day, yesterday, UserOptEntity.FLAG_SELF_EXAMINE_PASS, last7Day, yesterday, UserOptEntity.FLAG_APPLY, VisitEntity.FLAG_PLATFORM_PC), classOf[JsonObject])

    val mobileVisitorCountTopResp = DBHelper.find(s"SELECT v_source,COUNT(DISTINCT visitor_id) AS count FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ? AND occur_date <= ? AND c_platform = ? GROUP BY  v_source ORDER BY count DESC LIMIT 5 ", List(last7Day, yesterday, VisitEntity.FLAG_PLATFORM_MOBILE), classOf[JsonObject])

    val mobileSelfCountResp = DBHelper.find(
      s"""
         |SELECT source,COUNT(1) AS count FROM ${UserOptEntity.db.TABLE_NAME}
         |     WHERE occur_date >= ? AND occur_date <= ? AND action = ? AND id IN (
         |         SELECT id FROM ${UserOptEntity.db.TABLE_NAME} WHERE occur_date >= ? AND occur_date <= ? AND  action = ? AND platform = ?
         |     )  GROUP BY  source
       """.stripMargin, List(last7Day,yesterday, UserOptEntity.FLAG_SELF_EXAMINE_PASS, last7Day, yesterday, UserOptEntity.FLAG_APPLY, VisitEntity.FLAG_PLATFORM_MOBILE), classOf[JsonObject])

    val allVisitorCountResp = DBHelper.count(s"SELECT COUNT(DISTINCT visitor_id) FROM  ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ?  AND occur_date <= ?", List(last7Day, yesterday))
    val newVisitorCountResp = DBHelper.count(s"SELECT COUNT(DISTINCT visitor_id) FROM  ${VisitEntity.db.TABLE_NAME} WHERE v_new_visitor =? AND occur_date >= ? AND occur_date <= ?", List(1, last7Day, yesterday))

    val pcCountResp = DBHelper.count(s"SELECT COUNT(1) FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ? AND occur_date <= ? AND c_platform = ?", List(last7Day, yesterday, VisitEntity.FLAG_PLATFORM_PC))
    val mobileCountResp = DBHelper.count(s"SELECT COUNT(1) FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ? AND occur_date <= ? AND c_platform = ?", List(last7Day, yesterday, VisitEntity.FLAG_PLATFORM_MOBILE))

    for {
      pcVisitorTopCount <- pcVisitorCountTopResp
      pcSelfCount <- pcSelfCountResp
      mobileVisitorTopCount <- mobileVisitorCountTopResp
      mobileSelfCount <- mobileSelfCountResp
      allVisitorCount <- allVisitorCountResp
      newVisitorCount <- newVisitorCountResp
      pcCount <- pcCountResp
      mobileCount <- mobileCountResp
    } yield {
      val pcTrafficSourceTop = pcVisitorTopCount.body.map {
        visitorTop =>
          s"""
             |{
             |    "source":"${visitorTop.getString("v_source")}",
             |    "visitors":${visitorTop.getLong("count")},
             |    "conversions":${pcSelfCount.body.find(i => i.getString("source") == visitorTop.getString("v_source")).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count")}
             |}
           """.stripMargin
      }.mkString("[", ",", "]")
      val mobileTrafficSourceTop = mobileVisitorTopCount.body.map {
        visitorTop =>
          s"""
             |{
             |    "source":"${visitorTop.getString("v_source")}",
             |    "visitors":${visitorTop.getLong("count")},
             |    "conversions":${mobileSelfCount.body.find(i => i.getString("source") == visitorTop.getString("v_source")).getOrElse(new JsonObject( s"""{"count":0}""")).getLong("count")}
             |}
           """.stripMargin
      }.mkString("[", ",", "]")
      val result =
        s"""
           |{
           |    "pcTrafficSourceTop":$pcTrafficSourceTop,
           |    "mobileTrafficSourceTop":$mobileTrafficSourceTop,
           |    "newVisitorCount":${newVisitorCount.body},
           |    "oldVisitorCount":${allVisitorCount.body - newVisitorCount.body},
           |    "pcCount":${pcCount.body},
           |    "mobileCount":${mobileCount.body}
           |}
         """.stripMargin
      p.success(Resp.success(JsonHelper.toJson(result)))
    }
  }

}
