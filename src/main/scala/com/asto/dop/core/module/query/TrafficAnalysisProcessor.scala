package com.asto.dop.core.module.query

import java.text.DecimalFormat
import java.util.Date

import com.asto.dop.core.entity.{SourceFlagEntity, UserOptEntity, VisitEntity}
import com.asto.dop.core.helper.DBHelper
import com.ecfront.common.Resp
import io.vertx.core.json.JsonObject

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Promise

/**
 * 流量分析（以下所有的数据都为最近7天的数据）
 * 访客-自审通过率：访客，某段时间内有访问记录，不关心是不是新老访客，自审通过率，某段时间内申请并通过的记录
 */
object TrafficAnalysisProcessor extends QueryProcessor {

  override protected def process(req: Map[String, String], p: Promise[Resp[Any]]): Unit = {
    val today = df.format(new Date()).substring(0, 8).toLong
    val last7Day = today - 7
    val pcSourceCountResp = DBHelper.find(s"SELECT CASE WHEN b.source is null or b.source ='' THEN '元宝铺' ELSE b.source end AS v_source ," +
      s"COUNT(DISTINCT visitor_id) AS count FROM ${VisitEntity.db.TABLE_NAME} a LEFT JOIN ${SourceFlagEntity.db.TABLE_NAME}  b ON b.flag =a.v_source " +
      s"WHERE occur_date >= ? AND c_platform = ? GROUP BY b.source ", List(last7Day, VisitEntity.FLAG_PLATFORM_PC),classOf[JsonObject])

    val pcSelfCountResp = DBHelper.find(s"SELECT CASE WHEN b.source is null or b.source ='' THEN '元宝铺' ELSE b.source end AS v_source" +
      s",COUNT(1) AS count FROM  ${UserOptEntity.db.TABLE_NAME} a,${SourceFlagEntity.db.TABLE_NAME}  b " +
      s"WHERE a.source =b.flag AND action='bank_examine_pass' AND  occur_date >? and platform=? AND EXISTS " +
      s"(SELECT u_user_id FROM visit  WHERE u_user_id =a.user_id AND occur_date >= ? AND c_platform = ?) " +
      s"GROUP BY b.source",
      List(last7Day, VisitEntity.FLAG_PLATFORM_PC,last7Day, VisitEntity.FLAG_PLATFORM_PC),classOf[JsonObject])

    val mbSourceCountResp = DBHelper.find(s"SELECT CASE WHEN b.source is null or b.source ='' THEN '元宝铺' ELSE b.source end AS v_source ," +
      s"COUNT(DISTINCT visitor_id) AS count FROM ${VisitEntity.db.TABLE_NAME} a LEFT JOIN ${SourceFlagEntity.db.TABLE_NAME} b ON b.flag =a.v_source " +
      s"WHERE occur_date >= ? AND c_platform = ? GROUP BY b.source ", List(last7Day, VisitEntity.FLAG_PLATFORM_MOBILE),classOf[JsonObject])

    val mbSelfCountResp = DBHelper.find(s"SELECT CASE WHEN b.source is null or b.source ='' THEN '元宝铺' ELSE b.source end AS v_source" +
      s",COUNT(1) AS count FROM  ${UserOptEntity.db.TABLE_NAME} a,${SourceFlagEntity.db.TABLE_NAME}  b " +
      s"WHERE a.source =b.flag AND action='bank_examine_pass' AND  occur_date >? and platform=? AND EXISTS " +
      s"(SELECT u_user_id FROM visit  WHERE u_user_id =a.user_id AND occur_date >= ? AND c_platform = ?) " +
      s"GROUP BY b.source",
      List(last7Day, VisitEntity.FLAG_PLATFORM_MOBILE,last7Day, VisitEntity.FLAG_PLATFORM_MOBILE),classOf[JsonObject])

    val newVisitorCountResp = DBHelper.count(s"SELECT COUNT(distinct visitor_id) FROM  ${VisitEntity.db.TABLE_NAME} WHERE v_new_visitor =? AND occur_date >= ?",List(1,last7Day))
    val oldVisitorCountResp = DBHelper.count(s"SELECT COUNT(distinct visitor_id) FROM  ${VisitEntity.db.TABLE_NAME} WHERE v_new_visitor =? AND occur_date >= ? AND NOT EXISTS (SELECT visitor_id FROM ${VisitEntity.db.TABLE_NAME} WHERE v_new_visitor =?)",List(0,last7Day,1))

    val pcTrafficAnalysisCountResp = DBHelper.count(s"SELECT COUNT(1) FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ? AND c_platform = ?",List(last7Day,VisitEntity.FLAG_PLATFORM_PC))
    val mbTrafficAnalysisCountResp = DBHelper.count(s"SELECT COUNT(1) FROM ${VisitEntity.db.TABLE_NAME} WHERE occur_date >= ? AND c_platform = ?",List(last7Day,VisitEntity.FLAG_PLATFORM_MOBILE))

    var pcSourceJson :String =""
    var mbSourceJson :String =""

    for {
      pcSourceCount <- pcSourceCountResp
      pcSelfCount <- pcSelfCountResp
      mbSourceCount <- mbSourceCountResp
      mbSelfCount <- mbSelfCountResp
      newVisitorCount <- newVisitorCountResp
      oldVisitorCount <- oldVisitorCountResp
      pcTrafficAnalysisCount <- pcTrafficAnalysisCountResp
      mbTrafficAnalysisCount <- mbTrafficAnalysisCountResp
    } yield {
      val newVisitorProp = if(newVisitorCount.body + oldVisitorCount.body ==0) formatDouble("0.00",0) else formatDouble("0.00",newVisitorCount.body*100/(newVisitorCount.body +oldVisitorCount.body))
      val oldVisitorProp = if(newVisitorCount.body + oldVisitorCount.body ==0) formatDouble("0.00",0) else formatDouble("0.00",oldVisitorCount.body*100/(newVisitorCount.body +oldVisitorCount.body))
      val newVistorJson = s""""newVistor":{"count":"${newVisitorCount.body}","prop\":"$newVisitorProp%"}"""
      val oldVistorJson = s""""oldVistor":{"count":"${oldVisitorCount.body}","prop\":"$oldVisitorProp%"}"""

      val pcTrafficAnalysisProp = if(pcTrafficAnalysisCount.body + mbTrafficAnalysisCount.body ==0) formatDouble("0.00",0) else formatDouble("0.00",pcTrafficAnalysisCount.body*100/(pcTrafficAnalysisCount.body + mbTrafficAnalysisCount.body))
      val mbTrafficAnalysisProp = if(pcTrafficAnalysisCount.body + mbTrafficAnalysisCount.body ==0) formatDouble("0.00",0) else formatDouble("0.00",mbTrafficAnalysisCount.body*100/(pcTrafficAnalysisCount.body + mbTrafficAnalysisCount.body))

      val pcTrafficAnalysisJson =s""""pcVistor":{"count":${pcTrafficAnalysisCount.body},"prop\":"$pcTrafficAnalysisProp%"}"""
      val mbTrafficAnalysisJson =s""""mbVistor":{"count":${mbTrafficAnalysisCount.body},"prop\":"$mbTrafficAnalysisProp%"}"""


      val pcSourceList=pcSourceCount.body.map(i =>( i.getString("v_source"),i.getLong("count")))
      val pcSelfList = pcSelfCount.body.map(i =>( i.getString("v_source"),i.getLong("count")))
      val pcList = pcSourceList.map(f => f._1 -> (f._2, if (f._2 == 0) formatDouble("0.00", 0) else formatDouble("0.00", pcSelfList.find(_._1 == f._1).getOrElse((f._1, 0))._2.toString.toLong * 100 / f._2)))


      val mbSourceList = mbSourceCount.body.map(i =>( i.getString("v_source"),i.getLong("count")))
      val mbSelfList = mbSelfCount.body.map(i =>( i.getString("v_source"),i.getLong("count")))
      val mbList = mbSourceList.map(f => f._1 ->(f._2, if (f._2 == 0) formatDouble("0.00", 0) else formatDouble("0.00", mbSelfList.find(_._1 == f._1).getOrElse((f._1, 0))._2.toString.toLong * 100 / f._2)))
      for{
          pc <- pcList
      }yield{
        pcSourceJson =pcSourceJson+"{\"source\":\""+pc._1+"\",\"count\":"+pc._2._1+",\"prop\":\""+pc._2._2+"%\"},"
      }
      pcSourceJson = "\"PCData\":["+pcSourceJson.substring(0,pcSourceJson.length-1)+"]"
      for{
        mb <- mbList
      }yield{
        mbSourceJson =mbSourceJson+"{\"source\":\""+mb._1+"\",\"count\":"+mb._2._1+",\"prop\":\""+mb._2._2+"%\"},"
      }
      mbSourceJson = "\"MBData\":["+mbSourceJson.substring(0,mbSourceJson.length-1)+"]"
      p.success(Resp.success(
        s"""
           |{
           |$pcSourceJson,
           |$mbSourceJson,
           |$newVistorJson,
           |$oldVistorJson,
           |$pcTrafficAnalysisJson,
           |$mbTrafficAnalysisJson
           |}
        """.stripMargin))
    }
  }

  def formatDouble(patten :String ,s:Double ):String ={

    new DecimalFormat(patten).format(s)
  }
}
